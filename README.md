<h1>Introducing My Data Engieering Project</h1>

<h1>OverView</h1>

## 1 Process

- **_In this project I deploy an ETL process, initialize suitable storage and BI system for data of stock._** 
- **_The data in this project is extracted from data source of the US stock market._**
- **_I use various of tools including big data tools to perform complex tasks_**
- **_Integrate the automation workflow in this project to trigger tasks in right time_**
- **_Create dashboard model for analytics purpose__**


## 2 Project Architect

<img src="img\Project Architect.png" alt="ETL">  

## 3 Dashboards
https://drive.google.com/file/d/1jW4DsNfhnjfpGlTw9MevLFmh5RhHWkMV/view?usp=drive_link


<h1>Detail of the Project</h1>


- [A. Data Source And Design](#a-data-source-and-design)
  - [I. API Detail](#i-api-detail)
    - [1. Alpha Vantage API for market-status](#1-alpha-vantage-api-for-market-status)
    - [2. Alpha Vantage API for news\_sentiment](#22-alpha-vantage-api-for-news_sentiment)
    - [3. sec-api.io](#1-sec-apiio)
    - [4. Polygon](#4-polygon)
  - [II. Data from JSON](#ii-Data-from-json)
    - [1. Extract](#1-extract)
    - [2. Estimate](#2-estimate)
    [III. ETL Architecture](#v-etl-architecture)
  - [IV. ERD](#iii-erd)
  - [V. Star Schema](#iv-star-schema)


# A. Data Source And Design
## I. API Detail

Những dữ liệu mà các API cung cấp. 

### 1. Alpha Vantage API for market-status
[Alpha Vantage API](https://www.alphavantage.co/documentation/#market-status)
- API này giúp chúng ta truy xuất danh sách các thị trường chứng khoán trên toàn thế giới
- API này sẽ được extract mỗi tháng, nhưng có thể không cần thiết vì dữ liệu về thị trường và khu vực ít khi thay đổi
- Đây là các tham số API cần thiết để truy xuất trạng thái thị trường hiện tại của các sàn giao dịch chính cho cổ phiếu, ngoại hối và tiền điện tử trên toàn thế giới:
    - **function**: "MARKET_STATUS".
    - **apikey**: Khóa API để xác thực yêu cầu.
- Mọi thay đổi trong tương lai đều được Overwrite
  
**Dữ liệu này cung cấp thông tin về các khu vực và các sàn chứng khoán tại khu vực đó. Dưới đây là mô tả của các thuộc tính trong JSON:**
- **market_type**: Loại thị trường, ở đây là Equity (Chứng khoán).
- **region**: Khu vực của thị trường(Hoa Kỳ, Canada, Vương quốc Anh).
- **primary_exchanges**: Các sàn giao dịch chính của thị trường.
- **local_open**: Thời gian mở cửa địa phương của thị trường.
- **local_close**: Thời gian đóng cửa địa phương của thị trường.
- **current_status**: Tình trạng hiện tại của thị trường (bỏ qua)

### 2. Alpha Vantage API for news_sentiment
[Alpha Vantage API](https://www.alphavantage.co/documentation/#news-sentiment)
- API này giúp chúng ta truy xuất dữ liệu tin tức về thị trường, cùng với dữ liệu sentiment từ một loạt các nguồn tin hàng đầu trên toàn thế giới.
- API này sẽ được extract mỗi cuối ngày. Mỗi lần extract sẽ lấy tất cả các bài viết được xuất bản trong ngày tại thị trường Mỹ
- Dưới đây là các tham số API cần thiết để truy xuất dữ liệu tin tức về thị trường, cùng với dữ liệu sentiment từ một loạt các nguồn tin hàng đầu trên toàn thế giới, bao gồm cổ phiếu, tiền điện tử, ngoại hối và một loạt các chủ đề như chính sách tài khóa, sáp nhập và thâu tóm, IPO, v.v.
    - **function**: "NEWS_SENTIMENT".
    - **tickers**: Các ký hiệu cổ phiếu / tiền điện tử / ngoại hối. Ví dụ: tickers=IBM sẽ lọc các bài viết đề cập đến ký hiệu IBM; tickers=COIN,CRYPTO:BTC,FOREX:USD sẽ lọc các bài viết đồng thời đề cập đến Coinbase (COIN), Bitcoin (CRYPTO:BTC), và Đô la Mỹ (FOREX:USD) trong nội dung của chúng. **`<lấy các bài báo liên quan đến thị trường chứng khoán>`**
    - **topics**: Các chủ đề tin tức quan tâm. Ví dụ: topics=technology sẽ lọc các bài viết viết về lĩnh vực công nghệ; topics=technology,ipo sẽ lọc các bài viết đồng thời nói về công nghệ và IPO trong nội dung của chúng. Dưới đây là danh sách đầy đủ các chủ đề được hỗ trợ.
        - Blockchain: `blockchain`
        - Earnings: `earnings`
        - IPO: `ipo`
        - Mergers & Acquisitions: `mergers_and_acquisitions`
        - Financial Markets: `financial_markets`
        - Economy - Fiscal Policy (e.g., tax reform, government spending): `economy_fiscal`
        - Economy - Monetary Policy (e.g., interest rates, inflation): `economy_monetary`
        - Economy - Macro/Overall: `economy_macro`
        - Energy & Transportation: `energy_transportation`
        - Finance: `finance`
        - Life Sciences: `life_sciences`
        - Manufacturing: `manufacturing`
        - Real Estate & Construction: `real_estate`
        - Retail & Wholesale: `retail_wholesale`
        - Technology: `technology`
        - **`<sử dụng tất cả các topic>`**
    - **time_from** và **time_to**: Phạm vi thời gian của các bài báo, theo định dạng YYYYMMDDTHHMM. Ví dụ: time_from=20220410T0130. Nếu time_from được chỉ định nhưng time_to bị thiếu, API sẽ trả về các bài viết được xuất bản giữa giá trị time_from và thời gian hiện tại. `**<bắt đầu truy vấn dữ liệu từ tháng 1 năm 2024>**`
    - **sort**: Theo mặc định, sort=LATEST và API sẽ trả về các bài viết mới nhất đầu tiên. Cũng có thể thiết lập sort=EARLIEST hoặc sort=RELEVANCE. **`<sử dụng LATEST>`**
    - **limit**: Theo mặc định, limit=50 và API sẽ trả về tối đa 50 kết quả phù hợp. Cũng có thể thiết lập limit=1000 để xuất tối đa 1000 kết quả. **`<sử dụng 1000>`**
    - **apikey**: Khóa API để xác thực yêu cầu.

**File JSON trả về (Frequently Insert)**
Nguồn cung cấp tin tức hoặc phân tích thị trường, có thể là một trang web hoặc một dịch vụ.
- **title**: Tiêu đề của mục.
- **url**: Đường dẫn đến bài viết hoặc nguồn cung cấp.
- **time_published**: Thời gian xuất bản.
- **authors**: Tác giả của mục.
- **summary**: Tóm tắt nội dung của mục.
- **source**: Nguồn cung cấp thông tin.
- **source_domain**: Tên miền của nguồn cung cấp.
- **topics**: Danh sách các chủ đề liên quan đến mục.
    - **topic**: Chủ đề.
    - **relevance_score**: Điểm độ liên quan của bài viết đến chủ đề đó.
- **overall_sentiment_score**: Điểm cảm xúc tổng thể của bài báo.
- **overall_sentiment_label**: Nhãn mô tả cảm xúc tổng thể của bài báo.
- **ticker_sentiment**: Danh sách các cổ phiếu được đề cập trong bài báo và điểm cảm xúc của chúng.
    - **ticker**: Mã cổ phiếu.
    - **relevance_score**: Độ liên quan của bài báo đến cổ phiếu đó.
    - **ticker_sentiment_score**: Điểm cảm xúc của bài báo cho cổ phiếu đó.
    - **ticker_sentiment_label**: Nhãn mô tả cảm xúc của bài báo cho cổ phiếu đó.
- **Chỉ số cảm xúc (sentiment_score_definition):**
    | sentiment_score_definition | x <= -0.35 | -0.35 < x <= -0.15 | -0.15 < x < 0.15 | 0.15 <= x < 0.35 | x >= 0.35 |
    | -------------------------- | ---------- | ------------------ | ---------------- | ---------------- | --------- |
    | Label                      | Bearish    | Somewhat-Bearish   | Neutral          | Somewhat_Bullish | Bullish   |
- **Chỉ số liên quan (relevance_score_definition):** 0 < x <= 1, điểm số càng cao thì độ liên quan càng cao.

### 3. sec-api.io
[sec-api.io](https://sec-api.io/docs/mapping-api/list-companies-by-exchange)
- API này giúp chúng ta truy xuất danh sách các công ty hiện đang có mặt trên các thị trường chứng khoán Nyse, Nasdaq, Nysemkt và Bats
- API này sẽ được extract mỗi tháng, mỗi khi extract là mất 4 lần (Nyse, Nasdaq, Nysemkt, Bats)
- Dưới đây là các tham số API cần thiết để truy xuất danh sách các công ty có trên các thị trường chứng khoán NASDAQ, NYSE, NYSEMKT và NATS.
    - **exchange_name**: Tên của thị trường chứng khoán cần trích xuất công ty.
    - **api_key**: Khóa API để xác thực yêu cầu.
- Chỉ lấy các dữ liệu thuộc công ty của 4 sàn chứng khoán Hoa Kỳ là NYSE, NASDAQ, NYSEMKT (AMEX), BATS
- Mở rộng dự án tương lai:
    - Thuộc tính isDelisted của các công ty có thể thay đổi → cập nhật giá trị của thuộc tính này cho công ty đó
    - Có công ty mới được thêm vào sàn giao dịch → thêm công ty mới vào
- Lưu ý: chỉ có 100 lần extract


**Dưới đây là mô tả cho mỗi thuộc tính trong tập tin JSON được API trả về:**
  
  1. **name**: Tên công ty.
  2. **ticker**: Ký hiệu duy nhất được sử dụng để nhận biết công ty trên sàn giao dịch chứng khoán.
  3. **cik**: Mã CIK (Central Index Key) được cấp cho công ty bởi Ủy ban Chứng khoán và Giao dịch (SEC)..
  4. **cusip**: Số CUSIP (Committee on Uniform Securities Identification Procedures), một định danh duy nhất cho chứng khoán.
  5. **exchange**: Sàn giao dịch nơi cổ phiếu của công ty được giao dịch, như "NASDAQ".
  6. **isDelisted**: Một giá trị boolean cho biết liệu cổ phiếu của công ty đã bị loại khỏi sàn giao dịch hay không.
  7. **category**: Mô tả loại hoặc hạng mục của cổ phiếu của công ty, như "Cổ phiếu thường về nội địa" hoặc "Cổ phiếu thông thường ADR".
  8. **sector**: Ngành của công ty, ví dụ như "Dịch vụ Tài chính" hoặc "Chăm sóc sức khỏe".
  9. **industry**: Ngành cụ thể mà công ty hoạt động, như "Ngân hàng" hoặc "Công nghệ Sinh học".
  10. **sic**: Mã phân loại ngành công nghiệp tiêu chuẩn (SIC), phân loại loại hình kinh doanh chính của công ty.
  11. **sicSector**: Mô tả ngành dựa trên mã SIC, như "Tài chính Bảo hiểm Và Bất động sản" hoặc "Sản xuất".
  12. **sicIndustry**: Xác định ngành dựa trên mã SIC, như "Ngân hàng Thương mại Nhà nước" hoặc "Các loại thuốc".
  13. **famaSector**: Có thể liên quan đến các ngành mô hình Fama-French.
  14. **famaIndustry**: Có thể liên quan đến các ngành mô hình Fama-French.
  15. **currency**: Đơn vị tiền tệ mà cổ phiếu của công ty được giao dịch, như "USD" cho đô la Mỹ hoặc "EUR" cho euro.
  16. **location**: Địa điểm của trụ sở công ty, như "Florida, Hoa Kỳ" hoặc "Pháp".
  17. **id**: Một định danh duy nhất cho mục nhập công ty.

### 4. Polygon
[Polygon](https://polygon.io/docs/stocks/get_v2_aggs_ticker__stocksticker__range__multiplier___timespan___from___to)
- API này giúp chúng ta truy xuất giá mở cửa, cao nhất, thấp nhất và đóng cửa hàng ngày (OHLC) cho toàn bộ thị trường chứng khoán tại Mỹ.
- API này sẽ được extract mỗi đầu ngày, vì dữ liệu ngày hôm nay ngày mai mới có thể png" alt="Galaxy Schema">


