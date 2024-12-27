from pymongo import MongoClient
import mysql.connector
from datetime import datetime
import json
from bson.objectid import ObjectId
import time
import logging
import os

# Cấu hình logging
log_filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_filename,      # Tên file log
    filemode='a',               # Ghi thêm vào file (append)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Định dạng log
    level=logging.INFO,          # Mức log tối thiểu,
    encoding='utf-8'
)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Cấp độ log in ra màn hình
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)
# Kết nối MongoDB
try:
    # Kết nối tới MongoDB
    client = MongoClient('YOUR_URL_CONNECT_MONGODB')
    db = client['YOUR_DATABASE']
    logging.info("kết nối với mongodb thành công")
except Exception as e:
    logging.error('Lỗi kết nối MongoDB:', e)  
    


# Kết nối MySQL
try:    
    mysql_conn = mysql.connector.connect(
        host="YOUR_SERVER",
        user="YOUR_USER",
        password="YOUR_PASSWORD",
        database="YOUR_DATABASE",
        connection_timeout=60,
        autocommit=True
    )
    cursor = mysql_conn.cursor()
    logging.info("kết nối mysql thành công")
except Exception as e:
    logging.error('Lỗi kết nối MySQL:', e)  

# Hàm chuyển đổi CreateDate thành datetime đầy đủ
def convert_to_datetime(create_date):
    day = create_date.get("postDay", 1)
    month = create_date.get("postMonth", 1)
    year = create_date.get("postYear", 1970) 
    hour = create_date.get("postHour", 0)
    minute = create_date.get("postMinute", 0)
    second = 0  # Mặc định giây là 0 (MongoDB thường không lưu giây)
    return datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)

# Hàm lưu checkpoint vào file
def save_checkpoint(order_collection, last_id):
    try:
        # Đọc checkpoint hiện tại
        checkpoints = load_all_checkpoints()

        # Cập nhật checkpoint cho collection hiện tại
        checkpoints[order_collection] = str(last_id)

        # Ghi lại toàn bộ checkpoint
        with open("checkpointtest.json", "w") as f:
            json.dump(checkpoints, f,indent=4)
        logging.info(f"Checkpoint đã lưu cho {order_collection}: {last_id}")
    except Exception as e:
        logging.error(f"Lỗi khi lưu checkpoint: {e}")

def load_checkpoint(order_collection):
    try:
        # Đọc toàn bộ checkpoint từ file
        with open("checkpoint.json", "r") as f:
            checkpoints = json.load(f)
        # Lấy checkpoint cho collection hiện tại
        return checkpoints.get(order_collection)
    except (FileNotFoundError, json.JSONDecodeError):
        return None  # Nếu file không tồn tại hoặc bị lỗi, trả về None

def load_all_checkpoints():
    try:
        with open("checkpoint.json", "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    
failed_records = []
# Hàm lưu dữ liệu vào MySQL
def save_to_mysql(record):
    sql = """
    INSERT INTO data_kas_mongodb (
        _Id, billId, datatimepos, siteName, empName, catName, areaName,
        prodName, unit, qty, qtyCancel, price,amount_sauthue,
        BillDiscount, discName, amount,
        vatAmount,PayAmount, paymentType, paymentName, custName,status
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    billId=VALUES(billId),
    datatimepos=VALUES(datatimepos),
    siteName=VALUES(siteName),
    empName=VALUES(empName),
    catName=VALUES(catName),
    areaName=VALUES(areaName),
    prodName=VALUES(prodName),
    unit=VALUES(unit),
    qty=VALUES(qty),
    qtyCancel=VALUES(qtyCancel),
    price=VALUES(price),
    amount_sauthue=VALUES(amount_sauthue),
    BillDiscount=VALUES(BillDiscount),
    discName=VALUES(discName),
    amount=VALUES(amount),
    vatAmount=VALUES(vatAmount),
    PayAmount=VALUES(PayAmount),
    paymentType=VALUES(paymentType),
    paymentName=VALUES(paymentName),
    custName=VALUES(custName),
    status=VALUES(status);
    """
    qty = record.get("qty", 0)
    price = record.get("price", 0.0)
    amount_sauthue = qty * price
    datatimepos = convert_to_datetime(record["datatimepos"])
    values = (
        str(record["_id"]),
        record.get("billId"),              
        datatimepos,                          
        record.get("siteName"),       
        record.get("empName"),          
        record.get("catName"),
        record.get("areaName"),
        record.get("prodName"),            
        record.get("unit"),             
        qty,             
        record.get("qtyCancel", 0),       
        price,
        amount_sauthue,      
        record.get("BillDiscount", 0.0),    
        record.get("discName"),            
        record.get("amount", 0.0),          
        record.get("vatAmount", 0.0),
        record.get("PayAmount", 0.0),      
        record.get("paymentType"),           
        record.get("paymentName"),          
        record.get("custName"),                
        record.get("status",0)
    )

    try:
        # Xây dựng câu lệnh SQL hoàn chỉnh để debug
        formatted_sql = sql % values
        # logging.info(f"SQL Query: {formatted_sql}")
        # Thực thi câu lệnh SQL
        cursor.execute(sql, values)
        mysql_conn.commit()
    except mysql.connector.Error as e:
        billId = record.get("billId", "N/A")
        failed_records.append({
            "_id": str(record["_id"]),
            "billId": record.get("billId"),
            "error": str(e)
        })
        logging.error(f"Lỗi khi lưu vào MySQL (billId={billId}): {e}")
    except Exception as e:
        bill_id = record.get("billId", "N/A")
        failed_records.append({
            "_id": str(record["_id"]),
            "billId": record.get("billId"),
            "error": str(e)
        })
        logging.error(f"Lỗi không xác định (billId={billId}): {e}")
        
def get_collection_name(base_name):
    now = datetime.now()  # Lấy thời gian hiện tại
    return f"{base_name}{now.year}{now.month:02d}"

totalrow = 0
# Hàm xử lý batch dữ liệu
def process_batch():
    global totalrow
    order_collection = get_collection_name("YOUR_COLLECTION")
    payment_collection = get_collection_name("YOUR_COLLECTION")
    discount_collection = get_collection_name("YOUR_COLLECTION")
    last_id = load_checkpoint(order_collection)  # Đọc checkpoint
    query = {'_id': {'$gt': ObjectId(last_id)}} if last_id else {}  # Chỉ lấy dữ liệu mới
    batch_size = 5000  # Số lượng dữ liệu mỗi batch

    
    while True:
        batch = list(db[order_collection]
                     .find(query)
                     .sort('_id', 1)
                     .limit(batch_size))

        if not batch:
            break  # Dừng nếu không còn dữ liệu
        
        # Thực hiện pipeline lookup trên toàn batch        
        pipeline = [
            {'$match': {'_id': {'$in': [record['_id'] for record in batch]}}},
            {
                '$lookup': {
                    'from': payment_collection,
                    'localField': 'billId',
                    'foreignField': 'code',
                    'as': 'payment'
                }
            },
            {
                '$lookup': {
                    'from': discount_collection,
                    'localField': 'billId',
                    'foreignField': 'code',
                    'as': 'discount'
                }
            }, 
            {
                '$unwind': {
                    'path': '$payment',
                    'preserveNullAndEmptyArrays': True 
                }
            },
            {
                '$unwind': {
                    'path': '$discount',
                    'preserveNullAndEmptyArrays': True 
                }
            },    
            {
            '$addFields': {
                # Tạo một trường mới tên là `CreateDate` gom tất cả các key liên quan
                'datatimepos': {
                    "postHour": "$postHour",
                    "postMinute": "$postMinute",
                    "postDay": "$postDay",
                    "postMonth": "$postMonth",
                    "postYear": "$postYear",
                }
                }
            },
            
            {
                '$project': {
                    'billId': 1,
                    'datatimepos': 1,
                    'siteName': 1,
                    'empName': 1,
                    'catName': 1,
                    'areaName': 1,
                    'prodName': 1,
                    'unit': 1,
                    'qty': 1,
                    'qtyCancel': 1,
                    'price': 1,
                    'BillDiscount': 1,
                    'discName':'$discount.discName',
                    'amount': 1,
                    'vatAmount': 1,
                    'PayAmount': 1,
                    'paymentType':'$payment.paymentType',
                    'paymentName':'$payment.paymentName',
                    'custName':'$payment.custName',
                    'status':1
                }
            }
        ]

        try:
            enriched_data = list(db[order_collection].aggregate(pipeline))
            for enriched_record in enriched_data:
                save_to_mysql(enriched_record)
            
            # logging.error(enriched_record)
            
            totalrow += len(batch)
            # Lưu checkpoint nếu batch xử lý thành công
            last_id = batch[-1]['_id']
            save_checkpoint(order_collection, last_id)
            query = {'_id': {'$gt': last_id}}
            logging.info(f"Đã xử lý {len(batch)} bản ghi, Tổng số đã xử lý {totalrow}, checkpoint: {last_id}")
            # logging.error(f"Truy vấn MongoDB tiếp theo: {query}")
            
        except Exception as e:
            logging.error(f"Lỗi khi xử lý batch: {e}")
            break  # Dừng nếu gặp lỗi nghiêm trọng

# Theo dõi thay đổi realtime
def watch_changes():
    try:
        order_collection = get_collection_name("order-data")
        logging.info("Đang theo dõi thay đổi từ MongoDB...")
        with db[order_collection].watch() as stream:
            for change in stream:
                logging.info(f"Thay đổi phát hiện: {change}")
                if change["operationType"] in ["insert", "update", "replace", "delete"]:
                    process_batch()  # Xử lý ngay dữ liệu mới
    except Exception as e:
        logging.error(f"Lỗi Change Streams: {e}")

def monitor_changes():
    current_month = datetime.now().month
    while True:
        new_month = datetime.now().month
        if new_month != current_month:
            current_month = new_month
            logging.info("Tháng mới, cập nhật collection!")
            process_batch()


        watch_changes()
        time.sleep(360000)

def save_failed_records():
    # Kiểm tra nếu file đã tồn tại
    if os.path.exists("failed_records.json"):
        try:
            # Đọc nội dung hiện tại của file
            with open("failed_records.json", "r") as f:
                existing_records = json.load(f)  # Danh sách bản ghi cũ
        except (json.JSONDecodeError, FileNotFoundError):
            print("File lỗi không đọc được hoặc không tồn tại. Tạo file mới.")
            existing_records = []  # Nếu file không hợp lệ, bắt đầu danh sách trống
    else:
        existing_records = []  # Nếu file chưa tồn tại, bắt đầu danh sách trống

    # Thêm các bản ghi mới vào danh sách hiện tại
    existing_records.extend(failed_records)

    # Ghi lại toàn bộ danh sách vào file
    with open("failed_records.json", "w") as f:
        json.dump(existing_records, f, indent=4)  # Ghi với định dạng dễ đọc
        print(f"Đã lưu tổng cộng {len(existing_records)} bản ghi lỗi vào failed_records.json")

    # Xóa danh sách failed_records sau khi lưu
    failed_records.clear()

# Chạy toàn bộ hệ thống
if __name__ == "__main__":
    try:
        logging.info("Xử lý dữ liệu batch ban đầu...")
        process_batch()
        monitor_changes()
        watch_changes()  # Theo dõi realtime
    except KeyboardInterrupt:
        logging.info("Dừng chương trình...")
    finally:
        save_failed_records()
        cursor.close()
        mysql_conn.close()
