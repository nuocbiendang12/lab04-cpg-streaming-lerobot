"""Pure CPG extraction logic: AST nodes, CFG edges, DFG edges, call edges.

No Kafka/IO side effects here so it can be unit tested in isolation.
Uses the stdlib `ast` module (team's chosen CPG/AST library, see lab report
for the tradeoff discussion vs. tree-sitter / Joern).

Stability contract: node/edge ids are pure functions of (file_path, source
positions, type). Reprocessing an unchanged file yields byte-identical ids,
which is what makes the downstream Neo4j MERGE idempotent (Task 6).
"""
import ast
import hashlib
from dataclasses import dataclass, field
from typing import Optional


def make_id(*parts: str) -> str:
    digest = hashlib.sha256(":".join(parts).encode("utf-8")).hexdigest()
    return digest[:16]


def file_hash(source: str) -> str:
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def node_id(file_path: str, node: ast.AST) -> str:
    lineno = getattr(node, "lineno", 0)
    col = getattr(node, "col_offset", 0)
    return make_id(file_path, str(lineno), str(col), type(node).__name__)


def edge_id(source_id: str, target_id: str, edge_type: str) -> str:
    return make_id(source_id, target_id, edge_type)


@dataclass
class NodeEvent:
    node_id: str
    file_path: str
    node_type: str
    name: Optional[str]
    lineno: int
    col_offset: int
    end_lineno: Optional[int]
    parent_id: Optional[str]


@dataclass
class EdgeEvent:
    edge_id: str
    source_id: str
    target_id: str
    edge_type: str  # CFG | DFG | CALL
    file_path: str
    attrs: dict = field(default_factory=dict)


@dataclass
class MetadataEvent:
    file_path: str
    file_hash: str
    size_bytes: int
    loc: int
    num_functions: int
    num_classes: int
    num_nodes: int
    num_edges: int


NAMED_ATTR = {
    ast.FunctionDef: "name",
    ast.AsyncFunctionDef: "name",
    ast.ClassDef: "name",
    ast.Name: "id",
    ast.Attribute: "attr",
    ast.Global: None,
}


def _node_name(node: ast.AST) -> Optional[str]:
    for cls, attr in NAMED_ATTR.items():
        if isinstance(node, cls) and attr:
            return getattr(node, attr, None)
    return None


def extract_ast_nodes(file_path: str, tree: ast.AST):
    """Yield NodeEvent for every AST node, with parent_id linking to enclosing node."""
    nodes = []

    class Visitor(ast.NodeVisitor):
        def __init__(self):
            self.parent_stack = []

        def generic_visit(self, node):
            if not hasattr(node, "lineno"):
                # skip nodes without source position (e.g. Load/Store/ctx markers)
                super().generic_visit(node)
                return
            nid = node_id(file_path, node)
            parent_id = self.parent_stack[-1] if self.parent_stack else None
            nodes.append(NodeEvent(
                node_id=nid,
                file_path=file_path,
                node_type=type(node).__name__,
                name=_node_name(node),
                lineno=node.lineno,
                col_offset=node.col_offset,
                end_lineno=getattr(node, "end_lineno", None),
                parent_id=parent_id,
            ))
            self.parent_stack.append(nid)
            super().generic_visit(node)
            self.parent_stack.pop()

    Visitor().visit(tree)
    return nodes


def _stmt_lists(node: ast.AST):
    """Yield every statement-list body on a node (body/orelse/finalbody)."""
    for attr in ("body", "orelse", "finalbody"):
        stmts = getattr(node, attr, None)
        if stmts:
            yield stmts


def extract_cfg_edges(file_path: str, tree: ast.AST):
    """Simplified CFG: sequential edges within each statement block, plus
    entry edges from compound statements (If/For/While/Try/FunctionDef) into
    the first statement of each of their bodies.
    """
    edges = []

    def link_sequential(stmts):
        for a, b in zip(stmts, stmts[1:]):
            a_id, b_id = node_id(file_path, a), node_id(file_path, b)
            edges.append(EdgeEvent(
                edge_id=edge_id(a_id, b_id, "CFG"),
                source_id=a_id, target_id=b_id, edge_type="CFG",
                file_path=file_path,
            ))

    def visit(node):
        for stmts in _stmt_lists(node):
            link_sequential(stmts)
            if isinstance(node, (ast.If, ast.For, ast.While, ast.Try, ast.FunctionDef,
                                  ast.AsyncFunctionDef, ast.ClassDef, ast.Module)) and stmts:
                src_id = node_id(file_path, node)
                first_id = node_id(file_path, stmts[0])
                edges.append(EdgeEvent(
                    edge_id=edge_id(src_id, first_id, "CFG"),
                    source_id=src_id, target_id=first_id, edge_type="CFG",
                    file_path=file_path,
                ))
            for stmt in stmts:
                visit(stmt)

    visit(tree)
    return edges


def extract_dfg_edges(file_path: str, tree: ast.AST):
    """Simplified DFG: within each function/module scope, link a variable
    assignment (Store) to each subsequent read (Load) of the same name
    before it is reassigned.
    """
    edges = []

    def scope_dfg(scope_node):
        last_def = {}  # name -> node_id of most recent assignment
        for node in ast.walk(scope_node):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node is not scope_node:
                continue  # nested scopes handled by their own recursive call
            if isinstance(node, ast.Name):
                if isinstance(node.ctx, ast.Store):
                    last_def[node.id] = node_id(file_path, node)
                elif isinstance(node.ctx, ast.Load) and node.id in last_def:
                    def_id = last_def[node.id]
                    use_id = node_id(file_path, node)
                    edges.append(EdgeEvent(
                        edge_id=edge_id(def_id, use_id, "DFG"),
                        source_id=def_id, target_id=use_id, edge_type="DFG",
                        file_path=file_path, attrs={"var": node.id},
                    ))

    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef)):
            scope_dfg(node)

    return edges


def extract_call_edges(file_path: str, tree: ast.AST):
    """Call edges: from the enclosing function (or module) to each ast.Call
    site, tagged with the resolved callee name. Also links to the matching
    FunctionDef node_id when the callee is defined in the same file.
    """
    edges = []
    func_defs_by_name = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func_defs_by_name.setdefault(node.name, node_id(file_path, node))

    def callee_name(call: ast.Call) -> Optional[str]:
        func = call.func
        if isinstance(func, ast.Name):
            return func.id
        if isinstance(func, ast.Attribute):
            return func.attr
        return None

    def visit(node, enclosing_id):
        cur_enclosing = enclosing_id
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Module)):
            cur_enclosing = node_id(file_path, node) if not isinstance(node, ast.Module) else node_id(file_path, node)
        if isinstance(node, ast.Call):
            call_id = node_id(file_path, node)
            name = callee_name(node)
            edges.append(EdgeEvent(
                edge_id=edge_id(cur_enclosing, call_id, "CALL"),
                source_id=cur_enclosing, target_id=call_id, edge_type="CALL",
                file_path=file_path, attrs={"callee_name": name},
            ))
            if name and name in func_defs_by_name:
                target_def_id = func_defs_by_name[name]
                edges.append(EdgeEvent(
                    edge_id=edge_id(call_id, target_def_id, "CALL_RESOLVES_TO"),
                    source_id=call_id, target_id=target_def_id, edge_type="CALL_RESOLVES_TO",
                    file_path=file_path, attrs={"callee_name": name},
                ))
        for child in ast.iter_child_nodes(node):
            visit(child, cur_enclosing)

    visit(tree, node_id(file_path, tree))
    return edges


def extract_metadata(file_path: str, source: str, tree: ast.AST,
                      num_nodes: int, num_edges: int) -> MetadataEvent:
    num_functions = sum(1 for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)))
    num_classes = sum(1 for n in ast.walk(tree) if isinstance(n, ast.ClassDef))
    return MetadataEvent(
        file_path=file_path,
        file_hash=file_hash(source),
        size_bytes=len(source.encode("utf-8")),
        loc=source.count("\n") + 1,
        num_functions=num_functions,
        num_classes=num_classes,
        num_nodes=num_nodes,
        num_edges=num_edges,
    )


def parse_file(file_path: str, source: str):
    """Parse one file and return (nodes, cfg_edges, dfg_edges, call_edges, metadata).
    Raises SyntaxError on invalid Python source (caller emits to cpg.errors).
    """
    tree = ast.parse(source, filename=file_path)
    nodes = extract_ast_nodes(file_path, tree)
    cfg_edges = extract_cfg_edges(file_path, tree)
    dfg_edges = extract_dfg_edges(file_path, tree)
    call_edges = extract_call_edges(file_path, tree)
    all_edges = cfg_edges + dfg_edges + call_edges
    metadata = extract_metadata(file_path, source, tree, len(nodes), len(all_edges))
    return nodes, cfg_edges, dfg_edges, call_edges, metadata
