import pandas as pd
from pymongo import MongoClient
# 修改路径为你的数据文件位置


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

# 读取 CSV 文件（修改 path 为你的 CSV 文件路径）
#csv_file_path = "games.csv"
#df = pd.read_csv(csv_file_path)

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


# 统计价格区间分布
try:
    price_stats = collection.aggregate([
        {"$bucket": {
            "groupBy": "$price",  # 更新为正确的字段名
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

# 统计发行日期（新格式为 YYYY-MM-DD）
try:
    year_stats = collection.aggregate([
        {"$match": {"release_date": {"$ne": None, "$ne": ""}}},
        {"$project": {
            "year": {"$substr": ["$release_date", 0, 4]}  # 提取年份
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
        {"$unwind": "$developers"},  # 开发商是数组字段
        {"$group": {"_id": "$developers", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    
    print("\n主要开发商 (前10):")
    for dev in developers_stats:
        if dev["_id"]:
            print(f"{dev['_id']}: {dev['count']}款游戏")
except Exception as e:
    print("无法统计开发商:", e)

# 统计游戏类型分布
try:
    genres_stats = collection.aggregate([
        {"$unwind": "$genres"},  # genres是数组字段
        {"$group": {"_id": "$genres", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ])
    
    print("\n主要游戏类型 (前10):")
    for genre in genres_stats:
        if genre["_id"]:
            print(f"{genre['_id']}: {genre['count']}款游戏")
except Exception as e:
    print("无法统计游戏类型:", e)

# 统计评价情况
try:
    # 计算好评率
    review_stats = collection.aggregate([
        {"$match": {"positive": {"$type": "int"}, "negative": {"$type": "int"}}},
        {"$project": {
            "name": 1,
            "total_reviews": {"$add": ["$positive", "$negative"]},
            "positive_rate": {
                "$cond": [
                    {"$eq": [{"$add": ["$positive", "$negative"]}, 0]},
                    0,
                    {"$multiply": [{"$divide": ["$positive", {"$add": ["$positive", "$negative"]}]}, 100]}
                ]
            }
        }},
        {"$match": {"total_reviews": {"$gt": 100}}},  # 至少有100条评价
        {"$bucket": {
            "groupBy": "$positive_rate",
            "boundaries": [0, 50, 70, 80, 90, 95, 100],
            "default": "其他",
            "output": {"count": {"$sum": 1}}
        }}
    ])
    
    print("\n好评率分布 (至少100条评价):")
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
                "windows": "$windows",
                "mac": "$mac",
                "linux": "$linux"
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ])
    
    print("\n平台支持情况:")
    for platform in platform_stats:
        platforms = []
        if platform["_id"]["windows"] == "True":
            platforms.append("Windows")
        if platform["_id"]["mac"] == "True":
            platforms.append("Mac")
        if platform["_id"]["linux"] == "True":
            platforms.append("Linux")
        
        platform_str = ", ".join(platforms) if platforms else "无平台支持"
        print(f"{platform_str}: {platform['count']}款游戏")
except Exception as e:
    print("无法统计平台支持:", e)

# 统计估计拥有者人数分布
try:
    owners_stats = collection.aggregate([
        {"$group": {"_id": "$estimated_owners", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ])
    
    print("\n估计拥有者人数分布:")
    for owner_range in owners_stats:
        if owner_range["_id"]:
            print(f"{owner_range['_id']}: {owner_range['count']}款游戏")
except Exception as e:
    print("无法统计拥有者人数:", e)

# 查找最受欢迎的游戏（按评价总数和推荐数）
try:
    popular_games = collection.aggregate([
        {"$match": {"recommendations": {"$type": "int"}}},
        {"$sort": {"recommendations": -1}},
        {"$limit": 10},
        {"$project": {
            "name": 1,
            "recommendations": 1,
            "positive": 1,
            "negative": 1,
            "peak_ccu": 1
        }}
    ])
    
    print("\n最受欢迎的游戏 (前10, 按推荐数):")
    for game in popular_games:
        print(f"{game['name']}: {game.get('recommendations', 0)}推荐, {game.get('peak_ccu', 0)}峰值在线")
except Exception as e:
    print("无法查找最受欢迎的游戏:", e)

# 分析标签分布
try:
    # 先查看一个标签字段的结构
    sample = collection.find_one({"tags": {"$exists": True}})
    if sample and "tags" in sample:
        print("\n标签字段类型:", type(sample["tags"]))
        print("标签示例:", sample["tags"][:100] + "..." if len(str(sample["tags"])) > 100 else sample["tags"])
        
        # 尝试处理标签（假设是字符串形式的字典）
        if isinstance(sample["tags"], str) and sample["tags"].startswith("{") and sample["tags"].endswith("}"):
            tag_stats = collection.aggregate([
                {"$match": {"tags": {"$type": "string"}}},
                {"$project": {
                    "tag_str": {"$substr": ["$tags", 1, {"$subtract": [{"$strLenCP": "$tags"}, 2]}]}
                }},
                {"$addFields": {
                    "tag_pairs": {"$split": ["$tag_str", ","]}
                }},
                {"$unwind": "$tag_pairs"},
                {"$addFields": {
                    "tag_name": {"$trim": {"input": {"$arrayElemAt": [{"$split": ["$tag_pairs", ":"]}, 0]}}},
                }},
                {"$group": {"_id": "$tag_name", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 15}
            ])
            
            print("\n最常见的标签 (前15):")
            for tag in tag_stats:
                if tag["_id"] and tag["_id"] != "":
                    print("{}: {}款游戏".format(tag['_id'].strip('\'"'), tag['count']))
except Exception as e:
    print("无法分析标签:", e)
