<h1>Welcome to My DE Project</h1>

- **In this project I am performing and ETL process and initialize storage solution for stock data**.

- **The project on the data source of US stock market**.

  - [I. ETL Architecture](#i-etl-architecture)
  - [II. API Detail](#ii-api-detail)
    - [1. sec-api.io](#1-sec-apiio)
    - [2. Alpha Vantage API for market-status](#2-alpha-vantage-api-for-market-status)
    - [3. Alpha Vantage API for news\_sentiment](#3-alpha-vantage-api-for-news_sentiment)
    - [4. Polygon](#4-polygon)
  - [III. Extract from JSON](#iii-extract-from-json)
    - [1. Extract](#1-extract)
    - [2. Estimate](#2-estimate)
  - [IV. ERD](#iv-erd)
  - [V. Star Schema](#v-star-schema)


## I. ETL Architecture
<img src="img\ETL.png" alt="ETL">

## II. APIs Detail
Các APIs cung cấp các data gì? 
### 1. sec-api.io
[sec-api.io](https://sec-api.io/docs/mapping-api/list-companies-by-exchange)
- API này giúp chúng ta truy xuất danh sách các công ty hiện đang có mặt trên các thị trường chứng khoán Nyse, Nasdaq, Nysemkt và Bats
- API này sẽ được extract mỗi tháng, mỗi khi extract là mất 4 lần (Nyse, Nasdaq, Nysemkt, Bats)
- Dưới đây là các tham số API cần thiết để truy xuất danh sách các công ty có trên các thị trường chứng khoán NASDAQ, NYSE, NYSEMKT và NATS.
    - **exchange_name**: Tên của thị trường chứng khoán cần trích xuất công ty.
    - **api_key**: Khóa API để xác thực yêu cầu.
- Chỉ lấy các công ty của 4 sàn chứng khoán Hoa Kỳ là NYSE, NASDAQ, NYSEMKT (AMEX), BATS
- Dự kiến trong tương lai:
    - Thuộc tính isDelisted của các công ty có thể thay đổi → cập nhật giá trị của thuộc tính này cho công ty đó
    - Có công ty mới được thêm vào sàn giao dịch → thêm công ty mới vào
- Lưu ý: chỉ có 100 lần extract
  
**Đây là mô tả cho mỗi thuộc tính trong tập tin JSON được API trả về:**
  1. **name**: Tên công ty.
  2. **ticker**: Ký hiệu duy nhất được sử dụng để nhận biết công ty trên sàn giao dịch chứng khoán.
  3. **cik**: Mã CIK (Central Index Key) được cấp cho công ty bởi Ủy ban Chứng khoán và Giao dịch (SEC).
  4. **cusip**: Số CUSIP (Committee on Uniform Securities Identification Procedures), một định danh duy nhất cho chứng khoán.
  5. **exchange**: Sàn giao dịch nơi cổ phiếu của công ty được giao dịch, như "NASDAQ".
  6. **isDelisted**: Một giá trị boolean cho biết liệu cổ phiếu của công ty đã bị loại khỏi sàn giao dịch hay không.
  7. **category**: Mô tả loại hoặc hạng mục của cổ phiếu của công ty, như "Cổ phiếu thường về nội địa" hoặc "Cổ phiếu thông thường ADR".
  8. **sector**: Ngành của công ty(Dịch vụ Tài chính, Chăm sóc sức khỏe, Giải pháp Công Nghệ, ..).
  9. **industry**: Ngành cụ thể mà công ty hoạt động(Ngân hàng, Công nghệ sinh học, ..).
  10. **sic**: Mã phân loại ngành công nghiệp tiêu chuẩn (SIC), phân loại loại hình kinh doanh chính của công ty.
  11. **sicSector**: Mô tả ngành dựa trên mã SIC, như "Tài chính Bảo hiểm Và Bất động sản" hoặc "Sản xuất".
  12. **sicIndustry**: Xác định ngành dựa trên mã SIC, như "Ngân hàng Thương mại Nhà nước" hoặc "Các loại thuốc".
  13. **famaSector**: Có thể liên quan đến các ngành mô hình Fama-French.
  14. **famaIndustry**: Có thể liên quan đến các ngành mô hình Fama-French.
  15. **currency**: Đơn vị tiền tệ mà cổ phiếu của công ty được giao dịch, như "USD" cho đô la Mỹ hoặc "EUR" cho euro.
  16. **location**: Địa điểm của trụ sở công ty, như "Florida, Hoa Kỳ" hoặc "Pháp".
  17. **id**: Một định danh duy nhất cho mục nhập công ty.

### 2. Alpha Vantage API for market-status
[Alphlà nhà cung cấp hàng đầu các API có thể truy cập cho dữ liệu thị trường tài chính bao gồm cổ phiếu, FX và tiền tệ kỹ thuật số/tiền điện tử.
        - **Polygon.io**: Đây là một công ty cung cấp dữ liệu và API cho thị trường tài chính được thành lập vào năm 2017 hợp tác với một số tổ chức uy tín trong ngành tài chính, bao gồm Google, Microsoft, Goldman Sachs và Morgan Stanley.
        - 
## IV. ERD
- Cấu trúc CSDL của data staging được extract vào trước khi xử lý data để đưa vào Data Warehouse
<img src="img\ERD.png" alt="ERD">

## V. Star Schema
- Business Requirement #1
    Tạo Data Mart từ CSDL để theo dõi giá bán cũng như khối lượng cổ phiểu được giao dịch sau mỗi ngày giao dịch. 
- Business Requirement #2
    Tạo Data Mart từ CSDL để theo dõi các chuyên gia nói gì về từng cổ phiếu sau mỗi ngày giao dịch. 

<img src="img\starschema.png" alt="starschema">

