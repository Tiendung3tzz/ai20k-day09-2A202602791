# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Tống Trần Tiến Dũng` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602791` |
| GitHub | `Tiendung3tzz` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-09608` |

## Kết quả cuối

- `dataset_version` của bạn: `a0d8e7f15bd7`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `645 / 193 / 181`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Telemetry A có các cột `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`. Vì vậy khác nhau ở tên cột, đơn vị tốc độ (m/s và km/h), kiểu thời gian (ISO-8601 và epoch milliseconds), tên kinh/vĩ độ và tên các tín hiệu điều khiển. Bronze giữ A và B ở hai bảng riêng, mọi cột dạng text để bảo toàn dữ liệu gốc, có thể replay và truy nguyên schema drift trước khi chuẩn hóa.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Silver chuẩn hóa về một schema chung, đổi `spd_kmh` sang m/s, đổi `timestamp_ms` sang UTC timestamp và trừ độ lệch đồng hồ của B là 1500 ms trước khi join với video. Kết quả có 6149 dòng telemetry sạch; B bắt đầu ở `2025-10-08 14:30:00.009+00:00` sau khi hiệu chỉnh.

| drive | duplicate | speed_missing | speed_out_of_range |
|---|---:|---:|---:|
| drive_a | 20 | 30 | 3 |
| drive_b | 20 | 30 | 3 |

Tổng quarantine là 106 dòng. Đưa dòng lỗi vào `rejects` giữ lại bằng chứng và lý do để audit/replay, thay vì xóa im lặng làm mất khả năng giải thích chất lượng dữ liệu.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` chỉ cảnh báo vì khoảng trống tín hiệu vẫn có thể được dùng cho phân tích và không nhất thiết làm hỏng toàn bộ bản ghi. `in_range` là lỗi vì tốc độ ngoài miền làm sai join, thống kê và dataset downstream. Trong `break_gate.json`, test `in_range_silver_telemetry_speed_mps__70__0` bắt được 6 dòng; khi nới ngưỡng, kết quả là `PASS=5 WARN=1 ERROR=1`, còn khi khôi phục là `PASS=6 WARN=1 ERROR=0`. Quality gate vì vậy là cổng chặn lỗi nghiêm trọng, không chỉ là báo cáo.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Với `trip-00003`, số dòng trước chạy lại là 1246 và sau hai lần rerun vẫn là 1246; fingerprint đều là `6c7480dfde7e`. Một biến thể ghi bằng append tạo 120 dòng cho trip có 60 dòng, tức bị nhân đôi sau hai lần. Delta `overwrite` theo partition thay thế nguyên trip một cách nguyên tử và giữ commit history, nên retry sau lỗi giữa chừng an toàn hơn chuỗi delete rồi insert có thể dừng ở trạng thái nửa xóa, nửa ghi.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen là ảnh hỏng/không có nội dung và bị loại (`drive_a` 6, `drive_b` 4); khung tối vẫn có thể chứa cảnh hữu ích nên chỉ gắn cờ và giữ lại (`drive_a` 4, `drive_b` 61). Tôi chọn ngưỡng 2, đúng cấu hình pipeline, vì loại được lặp gần rõ ràng mà chưa quá mạnh tay. Xe chạy nhanh làm cảnh thay đổi nhanh nên nên dùng ngưỡng chặt hơn; xe chậm hoặc đứng yên có thể chịu ngưỡng lớn hơn.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Với chia theo khung hình, cả 26 trip đều bị chia cắt và cả 26 trip xuất hiện ở cả train và test. Với chia theo trip, số trip bị chia cắt là 0. Các frame liên tiếp trong cùng một trip rất giống nhau, nên để cùng trip ở train và test làm mô hình gần như nhìn thấy cùng cảnh/đoạn đường, khiến điểm test đẹp giả tạo và không đo được khả năng tổng quát sang chuyến mới.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` được băm từ nội dung có thứ tự (`frame_id|split`), nên cùng dữ liệu và cùng cách chia sẽ cho cùng version; nó không đổi chỉ vì chạy vào thời điểm khác. Hai lần export đều cho `a0d8e7f15bd7` (manifest SHA-256 là `ade7da96da332d85efc9bef0c3e5e771724f2477d6f80c76142dcdba917e4ed6`). Dataset của các bạn có thể khác vì MSSV được dùng khi gán trip vào train/val/test, kéo theo tập split và hash nội dung khác.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest ghi dataset `a0d8e7f15bd7` có 1019 frame, train/val/test là 645/193/181 và 18/4/4 trip. Lineage gồm `bronze.raw_telemetry_a` version 0, `bronze.raw_telemetry_b` 0, `bronze.trips_raw` 0, `bronze.video_files` 1, `silver.frames` 25, `silver.telemetry` 0, `silver.frames_flagged` 0 và `silver.frame_telemetry` 0.

Manifest còn lưu SHA-256 video A `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, video B `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`, `manifest_sha256` và luật drop/split. Các version Delta, hash input, version dataset và manifest là thông tin để tái tạo đúng dữ liệu sau 3 tháng.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip được chọn là `trip-09608`. Lần chạy đầu có 1 task mapped `silver_frames` chuyển đỏ (map index 25), log ghi `simulated crash while extracting trip-09608`; 7 task downstream ở trạng thái `upstream_failed`. Tôi clear task đỏ để chạy lại, sau đó clear 7 trạng thái downstream terminal để pipeline tiếp tục; cuối cùng 26/26 task `silver_frames` xanh và DAG thành công. Idempotency không tạo dòng trùng, fingerprint vẫn giữ nguyên và `dataset_version` vẫn `a0d8e7f15bd7`. Bằng chứng là `report/screenshots/fail_task_log.png`, `airflow_graph.png`, và các file `idempotency.json`/`run_summary.json`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba điểm sẽ chịu tải đầu tiên là: (1) decode video và ghi frame vào object storage, vốn bị giới hạn CPU/I/O; (2) Postgres/warehouse cùng các truy vấn dbt, vốn bị giới hạn ghi và scan/transform; (3) Airflow scheduler, metadata DB và số lượng mapped task, vốn tăng mạnh theo 500 xe. Tôi sẽ dùng worker/queue phân tán cho decode, object storage phân vùng và ingest song song, warehouse partitioning + incremental dbt, và Airflow executor/pool với metadata DB riêng để giới hạn tải và retry.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Tôi thêm câu SQL `select split, frames, trips, drives, avg_speed_kmh from marts.mart_split_balance order by split` vào dashboard `Day 9: video data pipeline`. Kết quả thật là `test: 181 frame, 4 trip, 2 drive, 48.2 km/h`, `train: 645, 18, 2, 56.0 km/h`, `val: 193, 4, 2, 70.3 km/h`. Câu hỏi cho thấy split được cân bằng theo trip và vẫn bao phủ cả hai drive; đồng thời val có tốc độ trung bình cao hơn train/test nên cần lưu ý khi diễn giải metric.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều bất ngờ nhất là chỉ một trip lỗi có thể làm 7 task downstream thành `upstream_failed`, nhưng retry theo partition vẫn giữ được tính idempotent. Tôi cũng thấy chia theo frame làm cả 26 trip rò rỉ giữa train và test. Chưa rõ nhất là mức ngưỡng dHash tối ưu khi dữ liệu có tốc độ thay đổi mạnh. Một lỗi pipeline có thể chưa bắt được là cùng một cảnh bị ghi lại với timestamp khác hoặc schema vẫn hợp lệ nhưng ý nghĩa đơn vị bị đổi; cần thêm kiểm tra nội dung/semantic drift.
