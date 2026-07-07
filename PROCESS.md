# Nhật ký thực hiện Lab 04 — CPG Streaming Pipeline (repo: lerobot)

Tài liệu này ghi lại toàn bộ quá trình thực hiện từ môi trường trắng (chưa có Docker/Java) cho đến khi hoàn thành Task 1 → Task 4. Mục tiêu là để đọc lại và hiểu **vì sao** mỗi quyết định kỹ thuật được đưa ra, không chỉ **làm gì**.

## 0. Chuẩn bị môi trường

Máy ban đầu **chưa có** Docker Desktop, JDK, chỉ có Python 3.12 (qua `py` launcher) và WSL2 (Ubuntu-22.04) đã bật sẵn.

Đã cài thêm:
- **Docker Desktop 4.80.0** (qua `winget install Docker.DockerDesktop`) — chạy trên nền WSL2 có sẵn.
- **Eclipse Temurin JDK 17** (qua `winget install EclipseAdoptium.Temurin.17.JDK`) — bắt buộc cho PySpark (Task 5).
- Python venv riêng trong `venv/` với các thư viện: `kafka-python`, `neo4j`, `pymongo`, `pyspark==3.5.1`, `jupyter-book`...

  > Lưu ý: `pip install pyspark` mặc định kéo về **pyspark 4.1.2** (dùng Scala 2.13), trong khi các package Kafka/Mongo connector tải qua Maven (`spark-sql-kafka-0-10_2.12`, `mongo-spark-connector_2.12`) build cho Scala 2.12 → lỗi `NoSuchMethodError` khi load. Phải ghim `pyspark==3.5.1` để khớp Scala 2.12.
- PySpark trên Windows cần `winutils.exe` + `hadoop.dll` (không có sẵn) → tải từ `cdarlint/winutils` (bản `hadoop-3.3.6`), đặt vào `hadoop/bin/`, set `HADOOP_HOME`.

## 1. Hạ tầng Docker (`docker-compose.yml`)

5 service:

| Service | Image | Vai trò |
|---|---|---|
| `zookeeper` | confluentinc/cp-zookeeper:7.6.1 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.6.1 | Message broker (port 9092 nội bộ, 29092 cho host) |
| `kafka-connect` | build tùy chỉnh từ `confluentinc/cp-kafka-connect:7.6.1` + cài thêm plugin `neo4j/kafka-connect-neo4j:5.1.7` qua `confluent-hub install` | Chạy Neo4j Sink Connector (Task 4) |
| `neo4j` | neo4j:5.24-community (kèm plugin APOC) | Graph DB |
| `mongodb` | mongo:7.0 | Document DB (Task 5) |

`connectors/Dockerfile` chỉ có 2 dòng: kế thừa image `cp-kafka-connect` gốc và chạy `confluent-hub install --no-prompt neo4j/kafka-connect-neo4j:5.1.7`.

## 2. Task 1 — Repository Cloning & File Discovery

- Shallow clone: `git clone --depth=1 https://github.com/huggingface/lerobot.git repos/lerobot`
- Script: [`src/file_discovery.py`](src/file_discovery.py)
  - Duyệt toàn bộ `*.py` bằng `Path.rglob`, loại `.git`.
  - Loại trừ (tuỳ chọn nhưng bật sẵn theo gợi ý đề bài): file tên `setup.py`, `conf.py`, `test_*.py`, `*_test.py`, hoặc nằm trong thư mục `tests/`/`test/`.
  - Ghi kết quả ra `data/discovered_files.json` (danh sách file + summary) để `parser_service.py` đọc lại ở Task 2.

**Kết quả thực tế chạy:**

```
Total .py files discovered: 746
  Included (non-test/setup):  543
  Excluded (test/setup/conf): 203
Total size: 8670.0 KB
By top-level directory:
  src                  487
  tests                202
  examples             54
  scripts              2
  setup.py             1
```

Con số 746/543 khớp với ước tính ban đầu.

## 3. Task 2 — Incremental CPG Parser Service

Tách làm 2 file để logic parse thuần (không phụ thuộc Kafka) có thể test độc lập:

- [`src/cpg_extractor.py`](src/cpg_extractor.py): logic trích xuất CPG, dùng module chuẩn `ast` (lựa chọn thay vì `tree-sitter`/`Joern` vì đủ để lấy AST/CFG/DFG/CALL theo yêu cầu đề bài, không cần cài thêm binary ngoài, và Python `ast` cho vị trí dòng/cột chính xác để sinh ID ổn định).
- [`src/parser_service.py`](src/parser_service.py): orchestrator — đọc từng file **một lần một** (bounded memory: chỉ giữ AST + list event của file hiện tại trong RAM), gọi extractor, rồi đẩy sự kiện qua `kafka_producer.py`.

### Sinh ID ổn định (idempotency cốt lõi)

```python
node_id = sha256(f"{file_path}:{lineno}:{col_offset}:{node_type}")[:16]
edge_id = sha256(f"{source_id}:{target_id}:{edge_type}")[:16]
```

Vì ID là hàm thuần của vị trí + loại node trong file, parse lại một file **không đổi nội dung** sẽ luôn ra đúng ID cũ → MERGE ở Neo4j không tạo trùng lặp. Đây chính là nền tảng cho Task 6 (không thực hiện trong phạm vi việc của bạn, nhưng thiết kế đã hỗ trợ sẵn).

### 4 loại trích xuất

1. **AST nodes**: duyệt toàn bộ cây bằng `NodeVisitor` tự viết có theo dõi `parent_stack` để gắn `parent_id`.
2. **CFG edges** (simplified): nối tuần tự các statement trong cùng một block (`body`/`orelse`/`finalbody`), cộng thêm cạnh từ node cha (If/For/While/Try/FunctionDef/Module) vào statement đầu tiên của block.
3. **DFG edges** (simplified): trong từng scope (Module hoặc FunctionDef), theo dõi lần gán biến gần nhất (`Store`) và nối tới mọi lần đọc (`Load`) tiếp theo trước khi biến bị gán lại.
4. **CALL edges**: mỗi `ast.Call` sinh 1 cạnh từ hàm bao quanh tới call-site, kèm `callee_name`; nếu tên hàm được gọi trùng với một `FunctionDef` trong cùng file, sinh thêm cạnh `CALL_RESOLVES_TO` trỏ tới định nghĩa đó.

### Kiểm thử hiệu năng

Test trên file lớn nhất repo (`modeling_molmoact2.py`, ~188KB): **18,852 node, 2,248 CFG, 4,282 DFG, 1,776 CALL edge — xử lý trong 0.27 giây**. Không có file nào gây lỗi cú pháp trong toàn bộ 543 file.

**Kết quả chạy full 543 file:**

```
[543/543] processed (127.3s elapsed)
Done. 543 files parsed OK, 0 errors, 459699 nodes, 199252 edges emitted.
```

## 4. Task 3 — Kafka Topic Design

Script: [`src/kafka_setup.py`](src/kafka_setup.py). 4 topic:

| Topic | Partitions | Lý do |
|---|---|---|
| `cpg.nodes` | 3 | Khối lượng lớn nhất (459,699 message) → cần song song hoá |
| `cpg.edges` | 3 | Cũng nhiều (199,252 message) |
| `cpg.metadata` | 1 | 1 message/file (543 tổng), khối lượng thấp |
| `cpg.errors` | 1 | Hiếm khi có dữ liệu (0 lỗi trong lần chạy thật) |

Mỗi message có `schema_version` (`"1.0"`) và `event_time` (ISO-8601 UTC) để tương thích ngược, theo đúng yêu cầu đề bài. Producer: [`src/kafka_producer.py`](src/kafka_producer.py).

**Message mẫu thực tế đã verify (đọc lại từ topic):**
```json
{
  "schema_version": "1.0",
  "event_time": "2026-07-07T03:39:51.940332+00:00",
  "event_type": "node",
  "node_id": "d854bbcf866364de",
  "file_path": "lerobot/examples/annotations/run_hf_job.py",
  "node_type": "Constant",
  "lineno": 16, "col_offset": 0,
  "parent_id": "798721cf0e6eb0b2"
}
```

## 5. Task 4 — Neo4j Kafka Connector Sink

Không dùng Spark ở bước này — nối trực tiếp Kafka → Neo4j qua **Neo4j Kafka Connector Sink** (chạy trong container `kafka-connect`), đúng yêu cầu đề bài "without an intermediate Spark layer".

Config: [`config/neo4j-sink-nodes.json`](config/neo4j-sink-nodes.json), [`config/neo4j-sink-edges.json`](config/neo4j-sink-edges.json), đăng ký qua REST API (`POST http://localhost:8083/connectors`).

Key config quan trọng (tìm ra bằng cách decompile class `SinkConfiguration` trong jar của connector, vì tài liệu không ghi rõ): mỗi topic dùng 1 property riêng dạng

```
neo4j.cypher.topic.<tên-topic> = <câu Cypher>
```

trong đó biến `event` tự động được bind với nội dung JSON của message.

### Node sink (đơn giản, dùng `MERGE`):
```cypher
MERGE (n:CPGNode {id: event.node_id})
SET n.file_path = event.file_path, n.node_type = event.node_type, ...
WITH n, event WHERE event.parent_id IS NOT NULL
MERGE (p:CPGNode {id: event.parent_id})
MERGE (p)-[:PARENT_OF]->(n)
```

### Edge sink — một bug thực tế đã gặp và cách xử lý

**Ý tưởng ban đầu**: vì `edge_type` (CFG/DFG/CALL/CALL_RESOLVES_TO) là giá trị runtime, Cypher không cho phép loại quan hệ động trong `MERGE (a)-[:TYPE]->(b)` thông thường → dùng thủ tục APOC:
```cypher
CALL apoc.merge.relationship(a, event.edge_type, {id: event.edge_id}, {...}, b, {}) YIELD rel
```
Test thủ công qua Python driver (`session.run(...)`) chạy **đúng**. Nhưng khi để connector thật thi hành: Kafka Connect vẫn commit offset bình thường (không lỗi, không nằm trong dead-letter), nhưng **Neo4j không hề tạo node/relationship nào** — kể cả 2 `MERGE` node ở đầu câu. Đã xác minh bằng cách gửi từng "probe message" đơn lẻ và kiểm tra trực tiếp trong Neo4j, cũng như bật `DEBUG` logger cho package `org.neo4j.connectors` — không có log nào xuất hiện, nghĩa là quá trình thực thi phía connector chưa bao giờ thực sự chạy câu lệnh, hoặc câu lệnh gọi thủ tục APOC bị "nuốt" âm thầm trong context thực thi riêng của connector.

**Cách khắc phục**: bỏ APOC hoàn toàn, dùng `FOREACH` + `CASE` để tạo quan hệ với loại **tĩnh** (biết trước 4 loại edge cố định), giữ nguyên tính idempotent bằng cách `MERGE` trên cặp `(a)-[:TYPE]->(b)` (tương đương khoá `edge_id` vì `edge_id` vốn là hàm của `(source_id, target_id, edge_type)`):

```cypher
MERGE (a:CPGNode {id: event.source_id})
MERGE (b:CPGNode {id: event.target_id})
FOREACH (_ IN CASE WHEN event.edge_type = 'CFG' THEN [1] ELSE [] END |
  MERGE (a)-[r:CFG]->(b) SET r.edge_id = event.edge_id, r.file_path = event.file_path)
FOREACH (_ IN CASE WHEN event.edge_type = 'DFG' THEN [1] ELSE [] END |
  MERGE (a)-[r:DFG]->(b) SET ...)
... (tương tự cho CALL, CALL_RESOLVES_TO)
```

Sau khi đổi, gửi probe message mới → relationship xuất hiện ngay lập tức trong Neo4j. Xác nhận hoạt động đúng.

### Idempotency

- Node: khoá `MERGE` trên `id` (chính là `node_id` ổn định).
- Edge: khoá `MERGE` trên cặp `(source_id, target_id, TYPE)` — về mặt logic tương đương khoá trên `edge_id`.
→ Replay lại cùng message (hoặc cùng file không đổi) sẽ không tạo bản ghi trùng, chỉ `SET` đè lại thuộc tính.

### Hiệu năng ingest

Do chiến lược "Cypher per-topic" của connector thực thi **1 câu Cypher cho mỗi message riêng lẻ** (không gộp batch), tốc độ ingest thực tế khoảng 1,000–1,500 msg/phút/connector trên máy local. Đã tăng `tasks.max` từ 1 lên 3 (bằng đúng số partition của topic) để chạy song song 3 task, giúp tăng thông lượng đáng kể. Với ~460K node + ~200K edge, việc đồng bộ hết vào Neo4j mất khoảng 1–2 giờ chạy nền — đây là điều bình thường với thiết kế "1 write transaction / message" chứ không phải lỗi.

**Kiểm tra trạng thái đồng bộ bất kỳ lúc nào:**
```bash
docker exec cpg-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group connect-neo4j-cpg-nodes-sink
docker exec cpg-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group connect-neo4j-cpg-edges-sink
```
Khi cột `LAG` về `0` ở mọi partition nghĩa là đã đồng bộ xong toàn bộ.

**Snapshot thực tế tại thời điểm viết tài liệu này** (đang chạy dở, không phải kết quả cuối):
```
Nodes so far: 160,658 / 459,699
Relationships by type: PARENT_OF: 113,340 | CFG: 33,633 | DFG: 31,803 | CALL: 17,603 | CALL_RESOLVES_TO: 2,359
```
Cả 5 loại quan hệ đều xuất hiện đúng như thiết kế, xác nhận cấu hình đã đúng và sẽ tiếp tục điền đầy khi connector chạy xong.

## 6. Danh sách lệnh để chạy lại từ đầu

```bash
# 1. Hạ tầng
docker compose up -d zookeeper kafka neo4j mongodb
docker compose build kafka-connect
docker compose up -d kafka-connect

# 2. Topic
python src/kafka_setup.py

# 3. Đăng ký Neo4j sink connectors
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors -d @config/neo4j-sink-nodes.json
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors -d @config/neo4j-sink-edges.json

# 4. Task 1: file discovery
python src/file_discovery.py

# 5. Task 2: parse + emit toàn bộ file
cd src && python parser_service.py
```

## 7. Vấn đề đã gặp & bài học (để viết phần "reflection" trong report)

1. **Docker Desktop không cài sẵn** → cài qua winget, cần khởi động thủ công lần đầu để hoàn tất setup WSL2.
2. **PySpark trên Windows thiếu `winutils.exe`/`HADOOP_HOME`** → tải bổ sung từ `cdarlint/winutils`.
3. **Mismatch phiên bản Scala** giữa pyspark mới nhất (4.x/Scala 2.13) và các connector package (`_2.12`) → phải ghim `pyspark==3.5.1`.
4. **Bug khó thấy nhất**: dùng thủ tục APOC (`apoc.merge.relationship`) bên trong Cypher sink của Neo4j Kafka Connector khiến ghi dữ liệu bị bỏ qua hoàn toàn mà **không hề có bất kỳ log lỗi nào** (kể cả bật DEBUG) — Kafka Connect vẫn commit offset như thể mọi thứ thành công. Debug bằng cách gửi "probe message" đơn lẻ, quan sát trực tiếp trạng thái Neo4j trước/sau, và thử nghiệm cùng câu Cypher thủ công qua driver Python để cô lập vấn đề. Giải pháp cuối: thay APOC bằng `FOREACH`/`CASE` thuần Cypher.
