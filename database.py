import pandas as pd
from pymongo import MongoClient
# 修改路径为你的数据文件位置
df = pd.read_csv('games.csv')

connection_url = (
    "mongodb://cxq91:Qq12345678@ac-lsikpgh-shard-00-00.wtghhj8.mongodb.net:27017,"
    "ac-lsikpgh-shard-00-01.wtghhj8.mongodb.net:27017,"
    "ac-lsikpgh-shard-00-02.wtghhj8.mongodb.net:27017/"
    "?replicaSet=atlas-nr1rzc-shard-0&ssl=true&authSource=admin"
)
client = MongoClient(connection_url)
# 指定要使用的数据库和集合（根据需要修改）
db = client["steamDB"]          # 例如: "steamDB"
collection = db["steam_games"]   
data_records = df.to_dict(orient="records")

#try:
#    result = collection.insert_many(data_records)
#    print(f"成功插入 {len(result.inserted_ids)} 条记录")
#except Exception as e:
#    print("导入数据时出现错误：", e)

# 查询集合中的文档数量
doc_count = collection.count_documents({})
print(f"集合中共有 {doc_count} 条记录")

# 查询集合的大小信息
stats = db.command("collStats", "steam_games")
size_bytes = stats["size"]
storage_size = stats["storageSize"]
index_size = stats.get("totalIndexSize", 0)

# 转换为MB显示
size_mb = size_bytes / (1024 * 1024)
storage_mb = storage_size / (1024 * 1024)
index_mb = index_size / (1024 * 1024)

print(f"数据大小: {size_mb:.2f} MB")
print(f"存储空间: {storage_mb:.2f} MB")
print(f"索引大小: {index_mb:.2f} MB")
print(f"总占用空间: {(storage_mb + index_mb):.2f} MB")

# 查看前5条数据示例
print("\n数据示例:")
for doc in collection.find().limit(5):
    print(doc)

# 更新类别统计代码，使其适应您的数据结构
try:
    # 使用实际存在的Categories字段
    category_stats = collection.aggregate([
        {"$project": {"categories_array": {"$split": ["$Categories", ","]}}},
        {"$unwind": "$categories_array"},
        {"$group": {"_id": "$categories_array", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    
    print("\n游戏分类统计 (前10):")
    for category in category_stats:
        print(f"{category['_id']}: {category['count']}款游戏")
except Exception as e:
    print("无法统计游戏分类:", e)

# 使用正确的Genres字段统计游戏类型
try:
    genres_stats = collection.aggregate([
        {"$project": {"genres_array": {"$split": ["$Genres", ","]}}},
        {"$unwind": "$genres_array"},
        {"$group": {"_id": "$genres_array", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    
    print("\n游戏类型统计 (前10):")
    for genre in genres_stats:
        print(f"{genre['_id']}: {genre['count']}款游戏")
except Exception as e:
    print("无法统计游戏类型:", e)

# 统计价格区间分布
try:
    price_stats = collection.aggregate([
        {"$bucket": {
            "groupBy": "$Price",
            "boundaries": [0, 1, 5, 10, 20, 30, 50, 100, 1000],
            "default": "其他",
            "output": {"count": {"$sum": 1}}
        }},
        {"$sort": {"_id": 1}}
    ])
    
    print("\n价格区间统计:")
    for price_range in price_stats:
        if price_range["_id"] == "其他":
            print(f"其他价格: {price_range['count']}款游戏")
        elif price_range["_id"] == 0:
            print(f"免费游戏: {price_range['count']}款")
        else:
            next_boundary_index = [0, 1, 5, 10, 20, 30, 50, 100, 1000].index(price_range["_id"]) + 1
            if next_boundary_index < len([0, 1, 5, 10, 20, 30, 50, 100, 1000]):
                next_boundary = [0, 1, 5, 10, 20, 30, 50, 100, 1000][next_boundary_index]
                print(f"{price_range['_id']}元 - {next_boundary}元: {price_range['count']}款游戏")
except Exception as e:
    print("无法统计价格区间:", e)

# 统计发行日期（需要处理日期格式）
try:
    # 假设Release date格式为"Feb 3, 2020"
    year_stats = collection.aggregate([
        {"$match": {"Release date": {"$ne": None, "$ne": ""}}},
        {"$project": {
            "year": {"$arrayElemAt": [{"$split": ["$Release date", ", "]}, 1]}
        }},
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ])
    
    print("\n发行年份统计:")
    for year_data in year_stats:
        if year_data["_id"]:
            print(f"{year_data['_id']}: {year_data['count']}款游戏")
except Exception as e:
    print("无法统计发行年份:", e)

# 统计开发商分布
try:
    developers_stats = collection.aggregate([
        {"$group": {"_id": "$Developers", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    
    print("\n主要开发商 (前10):")
    for dev in developers_stats:
        if dev["_id"]:
            print(f"{dev['_id']}: {dev['count']}款游戏")
except Exception as e:
    print("无法统计开发商:", e)

# 统计评价情况
try:
    # 计算好评率
    review_stats = collection.aggregate([
        {"$match": {"Positive": {"$type": "int"}, "Negative": {"$type": "int"}}},
        {"$project": {
            "total_reviews": {"$add": ["$Positive", "$Negative"]},
            "positive_rate": {
                "$cond": [
                    {"$eq": [{"$add": ["$Positive", "$Negative"]}, 0]},
                    0,
                    {"$multiply": [{"$divide": ["$Positive", {"$add": ["$Positive", "$Negative"]}]}, 100]}
                ]
            }
        }},
        {"$bucket": {
            "groupBy": "$positive_rate",
            "boundaries": [0, 50, 70, 80, 90, 95, 100],
            "default": "其他",
            "output": {"count": {"$sum": 1}, "games": {"$push": "$Name"}}
        }}
    ])
    
    print("\n好评率分布:")
    for rate_range in review_stats:
        if rate_range["_id"] == "其他":
            print(f"其他评分: {rate_range['count']}款游戏")
        else:
            next_boundary_index = [0, 50, 70, 80, 90, 95, 100].index(rate_range["_id"]) + 1
            if next_boundary_index < len([0, 50, 70, 80, 90, 95, 100]):
                next_boundary = [0, 50, 70, 80, 90, 95, 100][next_boundary_index]
                print(f"{rate_range['_id']}% - {next_boundary}%: {rate_range['count']}款游戏")
except Exception as e:
    print("无法统计好评率:", e)

# 统计平台支持情况
try:
    platform_stats = collection.aggregate([
        {"$group": {
            "_id": {
                "Windows": "$Windows",
                "Mac": "$Mac",
                "Linux": "$Linux"
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ])
    
    print("\n平台支持情况:")
    for platform in platform_stats:
        platforms = []
        if platform["_id"]["Windows"]:
            platforms.append("Windows")
        if platform["_id"]["Mac"]:
            platforms.append("Mac")
        if platform["_id"]["Linux"]:
            platforms.append("Linux")
        
        platform_str = ", ".join(platforms) if platforms else "无平台支持"
        print(f"{platform_str}: {platform['count']}款游戏")
except Exception as e:
    print("无法统计平台支持:", e)