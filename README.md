# Spark_Properties_RDD_Dataframe
## Properties, RDD

**1. Properties**
- Spark properties kiểm soát hầu hết các cài đặt ứng dụng và được cấu hình riêng cho từng ứng dụng. Các thuộc tính này có thể được đặt trực tiếp trên SparkConf được chuyển tới SparkContext của bạn. SparkConf cho phép bạn định cấu hình một số thuộc tính phổ biến (ví dụ: master URL và application name), cũng như các cặp key-value tùy ý thông qua phương thức set() .

**2. RDD giới thiệu chung**
- RDD ( Resilient Distributed Datasets) - tập dữ liệu phân tán có khả năng phục hồi là một cấu trúc dữ liệu cơ bản của Spark. Nó là một tập hợp các đối tượng được phân phối bất biến. Mỗi tập dữ liệu trong RDD được chia thành các phân vùng logic, có thể được tính toán trên các nút khác nhau của cụm. RDD có thể chứa các đối tượng Python, Java hoặc Scala bất kỳ, bao gồm các lớp do người dùng định nghĩa.
- Apache Về mặt hình thức, RDD là một tập hợp các bản ghi được phân vùng, chỉ đọc. RDD có thể được tạo thông qua các hoạt động xác định trên dữ liệu trên bộ lưu trữ ổn định hoặc các RDD khác. RDD là một tập hợp các phần tử chịu được lỗi có thể hoạt động song song.
- Apache Có hai cách để tạo RDD - song song một tập hợp hiện có trong chương trình trình điều khiển của bạn hoặc tham chiếu tập dữ liệu trong hệ thống lưu trữ bên ngoài, chẳng hạn như hệ thống tệp chia sẻ, HDFS, HBase hoặc bất kỳ nguồn dữ liệu nào cung cấp Định dạng đầu vào Hadoop.

<p align="center"><img src="http://image.slidesharecdn.com/youtubespk-141216130447-conversion-gate02/95/apache-spark-rdd-101-3-638.jpg"/></p>
<p align="center"> Các thành phần của RDD</p>

**3. Vấn đề chia sẻ dữ liệu chậm trong Mapreduce**

- Spark Sử dụng RDD để đạt được các hoạt động MapReduce nhanh hơn và hiệu quả hơn. Bởi MapReduce sử dụng rộng rãi để xử lý và tạo các bộ dữ liệu lớn với một thuật toán phân tán, song song trên một cụm. Nó cho phép người dùng viết các phép tính song song, sử dụng một tập hợp các toán tử cấp cao, mà không phải lo lắng về việc phân phối công việc và khả năng chịu lỗi. Nhưng vẫn có trường hợp muốn sử dụng lại dữ liệu giữa các lần tính toán (Ví dụ: giữa hai công việc MapReduce) là phải ghi nó vào hệ thống lưu trữ ổn định bên ngoài (Ví dụ - HDFS) mặc dù Mapreduce có cung cấp nhiều nội dung trừu tượng để truy cập tài nguyên tính toán của một cụm
- Cả hai ứng dụng Lặp lại và Tương tác đều yêu cầu chia sẻ dữ liệu nhanh hơn trên các công việc song song. Chia sẻ dữ liệu chậm trong MapReduce là do sao chép, tuần tự hóa và IO đĩa. Về hệ thống lưu trữ, hầu hết các ứng dụng Hadoop, chúng dành hơn 90% thời gian để thực hiện các thao tác đọc-ghi HDFS.

**4. Ứng dụng lặp lại và tương tác trên Mapreduce**
-  Ứng dụng lặp lại sử dụng lại các kết quả trung gian qua nhiều lần tính toán trong nhiều giai đoạn. Hình minh họa sau giải thích cách hoạt động của Mapreduce hiện tại trong khi thực hiện các hoạt động lặp lại trên MapReduce. Điều này phát sinh chi phí đáng kể do sao chép dữ liệu, I / O đĩa và tuần tự hóa, khiến hệ thống chậm.
-  
<p align="center"><img src="https://www.tutorialspoint.com/apache_spark/images/iterative_operations_on_mapreduce.jpg"/></p>
<p align="center"> <em>Hoạt động lặp lại của Mapreduce</em></p>

- Hoạt động tương tác trên Mapreduce diễn ra khi người dùng chạy các truy vấn đặc biệt trên cùng một tập con dữ liệu. Mỗi truy vấn sẽ thực hiện I / O đĩa trên bộ nhớ ổn định, có thể chi phối thời gian thực thi ứng dụng. Hình minh họa sau giải thích cách hoạt động của khung hiện tại khi thực hiện các truy vấn tương tác trên MapReduce.

<p align="center"><img src="https://www.tutorialspoint.com/apache_spark/images/interactive_operations_on_mapreduce.jpg"/></p>
<p align="center"> <em>Hoạt động tương tác trên Mapreduce</em></p>

**5. Chia sẻ dữ liệu bằng Spark RDD**
- Để giải quyết việc chia sẻ dữ liệu chậm trong MapReduce các nhà nghiên cứu đã phát triển một framework chuyên biệt có tên là Apache Spark. Mà cốt lõi là Resilient Distributed Datasets (RDD), nó hỗ trợ tính toán xử lý trong bộ nhớ. Điều này có nghĩa là, nó lưu trữ trạng thái bộ nhớ như một đối tượng trên các công việc và đối tượng có thể chia sẻ giữa các công việc đó. Chia sẻ dữ liệu trong bộ nhớ nhanh hơn mạng và Đĩa từ 10 đến 100 lần.
- Hình minh họa dưới đây cho thấy các hoạt động lặp lại trên Spark RDD. Nó sẽ lưu trữ các kết quả trung gian trong một bộ nhớ phân tán thay vì Ổ lưu trữ ổn định (Disk) và làm cho hệ thống nhanh hơn. Có một lưu ý nho nhỏ: Nếu bộ nhớ Phân tán (RAM) không đủ để lưu trữ các kết quả trung gian (Trạng thái công việc), thì nó sẽ lưu các kết quả đó trên đĩa.

<p align="center"><img src="https://www.tutorialspoint.com/apache_spark/images/iterative_operations_on_spark_rdd.jpg"/></p>
<p align="center"> <em>Hoạt động lặp lại trên Spark RDD</em></p>

- Hình minh họa này cho thấy các hoạt động tương tác trên Spark RDD. Nếu các truy vấn khác nhau được chạy lặp lại trên cùng một tập dữ liệu, thì dữ liệu cụ thể này có thể được lưu trong bộ nhớ để có thời gian thực thi tốt hơn.

<p align="center"><img src="https://www.tutorialspoint.com/apache_spark/images/interactive_operations_on_spark_rdd.jpg"/></p>
<p align="center"> <em>Hoạt động tương tác trên Spark RDD</em></p>

- Theo mặc định, mỗi RDD đã chuyển đổi có thể được tính toán lại mỗi khi bạn chạy một hành động trên đó. Tuy nhiên, bạn cũng có thể duy trì một RDD trong bộ nhớ, trong trường hợp đó Spark sẽ giữ các phần tử xung quanh trên cụm để truy cập nhanh hơn nhiều, vào lần tiếp theo bạn truy vấn nó. Ngoài ra còn có hỗ trợ cho các RDD lâu dài trên đĩa hoặc được sao chép qua nhiều nút
### DATAFRAME - KHUNG DỮ LIỆU ĐA NĂNG

**1. Dataframe là gì?**

- DataFrame là một tập hợp dữ liệu phân tán được tổ chức thành các cột được đặt tên. Về mặt khái niệm, nó tương đương với một bảng trong cơ sở dữ liệu quan hệ hoặc một khung dữ liệu trong R / Python, nhưng với các tối ưu hóa phong phú hơn. DataFrames có thể được xây dựng từ nhiều nguồn như: tệp dữ liệu có cấu trúc, bảng trong Hive, cơ sở dữ liệu (SQL) hoặc RDD hiện có.
- Ví dụ minh họa với Spark SQL:

<p align="center"><img src="https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2019/04/new-1-447x300.png"/></p>
<p align="center"> <em>Dataframe in Spark SQL</em></p>
- Tạo một DataFrame về nhân viên có Tên của nhân viên dưới dạng kiểu dữ liệu chuỗi, ID nhân viên là kiểu dữ liệu chuỗi, Số điện thoại của nhân viên dưới dạng kiểu dữ liệu số nguyên, Địa chỉ nhân viên dưới dạng chuỗi kiểu dữ liệu, Mức lương của nhân viên dưới dạng kiểu dữ liệu nổi. Dữ liệu của từng nhân viên được lưu theo từng hàng như hình trên.

**2. DataFrames được thiết kế để đa chức năng**

<div align = "justify">&nbsp;&nbsp;&nbsp;&nbsp; <b>   * Nhiều ngôn ngữ lập trình</b>
  
- Đặc tính tốt nhất của DataFrames trong Spark là hỗ trợ nhiều ngôn ngữ, giúp các lập trình viên từ các nền tảng lập trình khác nhau sử dụng dễ dàng hơn. DataFrames trong Spark hỗ trợ R - Ngôn ngữ lập trình, Python, Scala và Java.
<div align = "justify">&nbsp;&nbsp;&nbsp;&nbsp; <b>   * Nhiều nguồn dữ liệu</b>
- DataFrames trong Spark có thể hỗ trợ nhiều nguồn dữ liệu khác nhau.

<p align="center"><img src="https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2019/04/new-3-768x393.png"/></p>
<p align="center"> <em>Một số nguồn dữ liệu của DataFrame</em></p>

<div align = "justify">&nbsp;&nbsp;&nbsp;&nbsp; <b>   * Xử lý dữ liệu có cấu trúc và bán cấu trúc</b>
- Yêu cầu cốt lõi mà DataFrames được giới thiệu là xử lý Dữ liệu lớn một cách dễ dàng. DataFrames trong Spark sử dụng định dạng bảng để lưu trữ dữ liệu theo cách linh hoạt cùng với lược đồ cho dữ liệu mà nó đang xử lý.

<div align = "justify">&nbsp;&nbsp;&nbsp;&nbsp; <b>   * Slicing và Dicing dữ liệu</b>
- API DataFrame hỗ trợ Slicing và Dicing dữ liệu. Nó có thể thực hiện các thao tác như chọn và lọc theo hàng và cột. Dữ liệu thống kê luôn có xu hướng bị Thiếu giá trị, Vi phạm phạm vi và giá trị không liên quan. Người dùng có thể quản lý dữ liệu bị thiếu một cách rõ ràng bằng cách sử dụng DataFrames.

**3. Các tính năng của DataFrame trong Spark**
<p align="center"><img src="https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2019/04/Picture11.png"</em></p>

- DataFrame trong spark có bản chất là Bất biến. Giống như Tập dữ liệu được phân phối có khả năng phục hồi, dữ liệu có trong DataFrame không thể bị thay đổi.
- Việc lười đánh giá là chìa khóa cho hiệu suất đáng chú ý do Spark mang lại. DataFrames trong Spark sẽ không hiển thị đầu ra trên màn hình trừ khi một thao tác hành động được kích hoạt.
- Kỹ thuật Bộ nhớ phân tán được sử dụng để xử lý dữ liệu làm cho chúng có khả năng chịu lỗi.
- Giống như Tập dữ liệu phân tán có khả năng phục hồi, DataFrames trong Spark mở rộng thuộc tính của mô hình bộ nhớ phân tán. Cách duy nhất để thay đổi hoặc sửa đổi dữ liệu trong DataFrame sẽ là áp dụng Chuyển đổi.
**4. Nguồn cho Spark Data Frame**
<p align="center"><img src="https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2019/04/new-2-768x448.png"</em></p>

- Có rất nhiều cách để tạo DataFrame trong Spark như:
- Dữ liệu có thể được tải vào thông qua CSV, JSON, XML, SQL, RDBMS và nhiều hơn nữa. Nó cũng có thể được tạo bằng cách sử dụng RDD hiện có và thông qua bất kỳ cơ sở dữ liệu nào khác, như Hive , HBase , Cassandra . Nó cũng có thể lấy dữ liệu từ HDFS hoặc hệ thống tệp cục bộ.

#### TÀI LIỆU THAM KHẢO
1.	https://www.tutorialspoint.com/apache_spark/apache_spark_rdd.htm
2.	https://www.edureka.co/blog/dataframes-in-spark
3.	http://www.gabormelli.com/RKB/Spark_RDD_Data_Structure
4.	https://data-flair.training/blogs/apache-spark-rdd-vs-dataframe-vs-dataset/
5.	Learning Spark by Materi Zaharia, Patrick Wendell, Andy Konwinski, Holden Karau



