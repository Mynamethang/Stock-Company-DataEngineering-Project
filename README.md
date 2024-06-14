<h1>Data Engineering Project</h1>


- **In this project I perform ETL process and initialize suitable data storage for the data.** 
- **The project on data sources of the US stock market**


- [A. Data Source And Design](#a-data-source-and-design)
  - [I. API Detail](#i-api-detail)
    - [1. sec-api.io](#1-sec-apiio)
    - [2. Alpha Vantage API for market-status](#2-alpha-vantage-api-for-market-status)
    - [3. Alpha Vantage API for news\_sentiment](#3-alpha-vantage-api-for-news_sentiment)
    - [4. Polygon](#4-polygon)
  - [II. Extract from JSON](#ii-extract-from-json)
    - [1. Extract](#1-extract)
    - [2. Estimate](#2-estimate)
  - [III. ETL Architecture](#v-etl-architecture)
  - [IV. ERD](#iii-erd)
  - [V. Star Schema](#iv-star-schema)
- [B. Implement a data workflow and essential tools for the process](#b-data-workflow)


# A. Data Source And Design
## I. API Detail
Đầu tiên chúng ta hãy cùng xem qua API được cung cấp là gì. 
### 1. sec-api.io
[sec-api.io](https://sec-api.io/docs/mapping-api/list-companies-by-exchange)
- API này giúp chúng ta truy xuất danh sách các công ty hiện đang có mặt trên các thị trường chứng khoán Nyse, Nasdaq, Nysemkt vàlượng liệu dựa trên các yếu tố sau:
    
    - a. **Dung lượng trung bình của mỗi dòng dữ liệu:**
        - Bảng giao dịch hằng ngày: Mỗi dòng dữ liệu bao gồm thông tin về một giao dịch, bao gồm mã chứng khoán, giá, khối lượng, thời gian giao dịch, v.v. Giả sử trung bình mỗi dòng dữ liệu có thể cần khoảng 100 byte ~ 0.1 KB để lưu trữ.
        - Bảng các bài báo: Mỗi bài báo bao gồm tiêu đề, nội dung, tác giả, ngày xuất bản, tóm tắt v.v. Giả sử trung bình mỗi bài báo có thể cần khoảng 1 kilobyte (KB) để lưu trữ.
        - Giả sử mỗi khi bảng các bài báo được ghi thì NEWS_Fact_Table cũng được cập nhật với môi dòng là 0,5 KB.
    - b. **Tần suất truy vấn dữ liệu:**
        - Bảng giao dịch hằng ngày: 10400 dòng dữ liệu được truy vấn mỗi ngày.
        - Bảng các bài báo: 10000 bài viết được truy vấn mỗi ngày.
        - Bảng khu vực: Cập nhật mỗi tháng.
        - Bảng thông tin về công ty: Cập nhật mỗi tháng.
    - c. **Tính toán:**
        - **Bảng giao dịch hằng ngày:**
            - Dung lượng trung bình mỗi dòng: 0.1 KB
            - Số lượng dòng dữ liệu mỗi ngày: 10400
            - Dung lượng lưu trữ cần thiết: 0.1 KB/dòng * 10400 dòng/ngày ≈ 1040 KB
        - **Bảng các bài báo:**
            - Dung lượng trung bình mỗi bài báo: 1 KB
            - Mỗi bài báo liên quan tới 3 cổ phiếu chứng khoán: 0.5 KB
            - Số lượng bài báo mỗi ngày: 1000
            - Dung lượng lưu trữ cần thiết: 1.5 KB/bài báo * 10000 bài báo/ngày ≈ 15000 KB
        - **Tổng dung lượng lưu trữ cần thiết trong một ngày là: 16040 KB ~ 15.66 MB**
        - **Tổng dung lượng lưu trữ cần thiết trong một tháng là: 469.92 MB**
        - **Tổng dung lượng lưu trữ cần thiết trong một năm là: 5,639.06 MB ~ 5.5 GB**
          
- **Velocity (Tốc độ của dữ liệu)**:
    - **Giao dịch cổ phiếu:** Mỗi giây, hàng triệu giao dịch cổ phiếu được thực hiện trên các sàn giao dịch toàn cầu. Dữ liệu giao dịch này cần được thu thập và xử lý trong thời gian thực để cung cấp cho các nhà đầu tư thông tin cập nhật về giá cả và khối lượng giao dịch.
    - **Tin tức và dữ liệu tài chính:** Các nguồn tin tức và dữ liệu tài chính liên tục tạo ra một lượng lớn dữ liệu mới, bao gồm bài báo, báo cáo, phân tích và ý kiến. Dữ liệu này cần được xử lý và phân tích nhanh chóng để giúp các nhà đầu tư đưa ra quyết định sáng suốt.
- **Variety (Đa dạng của dữ liệu)**:
    - **Dữ liệu giá cả:** Giá cổ phiếu theo thời gian thực và lịch sử, khối lượng giao dịch, giá mở, cao, thấp, đóng cửa, biến động giá
    - **Dữ liệu thị trường:** Chỉ số thị trường (VNIndex, VIX, v.v.), Tin tức và thông báo công ty, Báo cáo tài chính và phân tích, Thông tin về thị trường, Các bài báo với nhiều chủ đề.
- **Veracity (Tính chính xác của dữ liệu)**:
    - Nguồn dữ liệu được lấy từ các tổ chức uy tín như:
        - **SEC EDGAR(Electronic Data Gathering, Analysis, and Retrieval)**: là một hệ thống điện tử được Ủy ban Chứng khoán và Giao dịch Hoa Kỳ (SEC) sử dụng để thu thập, phân tích và truy xuất các tài liệu được nộp bởi các công ty đại chúng và các tổ chức khác.
        - **Alpha Vantage Inc.**: là nhà cung cấp hàng đầu các API có thể truy cập cho dữ liệu thị trường tài chính bao gồm cổ phiếu, FX và tiền tệ kỹ thuật số/tiền điện tử.
        - **Polygon.io**: là một công ty cung cấp dữ liệu và API cho thị trường tài chính được thành lập vào năm 2017 hợp tác với một số tổ chức uy tín trong ngành tài chính, bao gồm Google, Microsoft, Goldman Sachs và Morgan Stanley.
          
## III. ETL Architecture
<img src="img\ETL.png" alt="ETL">       

## IV. ERD
- Cấu trúc CSDL của data staging được extract vào trước khi xử lý data để đưa vào Data Warehouse
<img src="img\ERD.png" alt="ERD">

## V. Star Schema
- Business Requirement #1
    Tạo Data Mart từ CSDL để theo dõi giá bán cũng như khối lượng cổ phiểu được giao dịch sau mỗi ngày giao dịch. 
- Business Requirement #2
    Tạo Data Mart từ CSDL để theo dõi các chuyên gia nói gì về từng cổ phiếu sau mỗi ngày giao dịch. 

<img src="img\starschema.png" alt="starschema">


