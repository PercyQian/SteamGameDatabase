import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import pandas as pd
from pymongo import MongoClient
import json
import re
from functools import partial
from bson.objectid import ObjectId  # 导入这个，用于处理MongoDB的ObjectId

class SteamDatabaseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Steam游戏数据库查询工具")
        self.root.geometry("1200x800")
        self.root.configure(bg="#2a475e")  # Steam深蓝色背景
        
        # 设置样式
        self.setup_styles()
        
        # 连接到MongoDB
        self.connect_to_mongodb()
        
        # 获取所有可用标签
        self.all_tags = self.get_all_tags()
        
        # 创建UI元素
        self.create_ui()
        
        # 显示初始数据
        self.search_games()
    
    def setup_styles(self):
        """设置自定义样式"""
        style = ttk.Style()
        # 为游戏类型复选框创建特殊样式
        style.configure("Genre.TCheckbutton", foreground="blue")
    
    def connect_to_mongodb(self):
        try:
            connection_url = (
                "mongodb://cxq91:Qq12345678@ac-lsikpgh-shard-00-00.wtghhj8.mongodb.net:27017,"
                "ac-lsikpgh-shard-00-01.wtghhj8.mongodb.net:27017,"
                "ac-lsikpgh-shard-00-02.wtghhj8.mongodb.net:27017/"
                "?replicaSet=atlas-nr1rzc-shard-0&ssl=true&authSource=admin"
            )
            self.client = MongoClient(connection_url)
            self.db = self.client["steamDB"]
            self.collection = self.db["steam_games"]
            print("MongoDB连接成功")
        except Exception as e:
            messagebox.showerror("连接错误", f"无法连接到MongoDB: {e}")
    
    def get_all_tags(self):
        """获取所有可用的标签"""
        try:
            # 尝试从tags字段中获取所有可能的标签
            pipeline = [
                {"$match": {"tags": {"$exists": True, "$ne": ""}}},
                {"$limit": 1000}  # 限制处理的文档数量以提高性能
            ]
            
            cursor = self.collection.aggregate(pipeline)
            all_tags = set()
            
            for doc in cursor:
                tags = doc.get("tags", "")
                if isinstance(tags, str) and tags.startswith("{"):
                    try:
                        # 解析标签字符串成字典
                        tag_dict = json.loads(tags.replace("'", "\""))
                        # 添加所有标签到集合中
                        all_tags.update(tag_dict.keys())
                    except:
                        pass
            
            return sorted(list(all_tags))
        except Exception as e:
            print(f"获取标签时出错: {e}")
            return []
    
    def get_all_genres(self):
        """获取所有游戏类型"""
        try:
            genres = self.collection.distinct("genres")
            # 如果genres是字符串数组，直接返回
            if genres and isinstance(genres[0], str):
                return sorted(genres)
            
            # 如果genres是数组字段，需要展平
            flat_genres = []
            for genre_list in genres:
                if isinstance(genre_list, list):
                    flat_genres.extend(genre_list)
            
            return sorted(list(set(flat_genres)))
        except Exception as e:
            print(f"获取游戏类型时出错: {e}")
            return []
    
    def create_ui(self):
        # 创建主框架
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 分为左右两部分
        left_frame = ttk.Frame(main_frame, width=400)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, padx=5, pady=5)
        
        right_frame = ttk.Frame(main_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 左侧筛选区域
        filter_frame = ttk.LabelFrame(left_frame, text="筛选选项")
        filter_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 搜索框
        search_frame = ttk.Frame(filter_frame)
        search_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(search_frame, text="游戏名称:").pack(side=tk.LEFT)
        self.search_entry = ttk.Entry(search_frame)
        self.search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        # 价格范围
        price_frame = ttk.Frame(filter_frame)
        price_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(price_frame, text="价格范围:").pack(side=tk.LEFT)
        self.min_price_var = tk.StringVar(value="0")
        self.max_price_var = tk.StringVar(value="100")
        
        ttk.Label(price_frame, text="从").pack(side=tk.LEFT)
        ttk.Entry(price_frame, textvariable=self.min_price_var, width=5).pack(side=tk.LEFT)
        ttk.Label(price_frame, text="到").pack(side=tk.LEFT)
        ttk.Entry(price_frame, textvariable=self.max_price_var, width=5).pack(side=tk.LEFT)
        
        # 排序选项
        sort_frame = ttk.Frame(filter_frame)
        sort_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(sort_frame, text="排序方式:").pack(side=tk.LEFT)
        sort_options = ["游玩人数 (高到低)", "发布日期 (新到旧)", "价格 (低到高)", "评价 (好到差)", "名称 (A-Z)"]
        self.sort_var = tk.StringVar(value=sort_options[0])
        sort_dropdown = ttk.Combobox(sort_frame, textvariable=self.sort_var, values=sort_options, state="readonly", width=15)
        sort_dropdown.pack(side=tk.LEFT, padx=5)
        
        # 标签选择（多选框）
        tags_frame = ttk.LabelFrame(filter_frame, text="游戏标签 (可多选)")
        tags_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 添加标签搜索框
        tag_search_frame = ttk.Frame(tags_frame)
        tag_search_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(tag_search_frame, text="搜索标签:").pack(side=tk.LEFT)
        self.tag_search_var = tk.StringVar()
        self.tag_search_var.trace("w", self.filter_tags)
        tag_search_entry = ttk.Entry(tag_search_frame, textvariable=self.tag_search_var)
        tag_search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        # 创建标签滚动区域 (包含水平和垂直滚动条)
        tags_outer_frame = ttk.Frame(tags_frame)
        tags_outer_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 垂直滚动条
        tags_canvas = tk.Canvas(tags_outer_frame, height=200)
        tags_v_scrollbar = ttk.Scrollbar(tags_outer_frame, orient="vertical", command=tags_canvas.yview)
        # 水平滚动条
        tags_h_scrollbar = ttk.Scrollbar(tags_outer_frame, orient="horizontal", command=tags_canvas.xview)
        
        self.tags_scrollable_frame = ttk.Frame(tags_canvas)
        
        # 配置滚动区域
        self.tags_scrollable_frame.bind(
            "<Configure>",
            lambda e: tags_canvas.configure(scrollregion=tags_canvas.bbox("all"))
        )
        
        # 创建窗口并配置滚动条
        tags_canvas.create_window((0, 0), window=self.tags_scrollable_frame, anchor="nw")
        tags_canvas.configure(yscrollcommand=tags_v_scrollbar.set, xscrollcommand=tags_h_scrollbar.set)
        
        # 布局滚动区域组件
        tags_v_scrollbar.pack(side="right", fill="y")
        tags_h_scrollbar.pack(side="bottom", fill="x")
        tags_canvas.pack(side="left", fill="both", expand=True)
        
        # 添加已选标签显示区域
        selected_tags_frame = ttk.LabelFrame(tags_frame, text="已选标签")
        selected_tags_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.selected_tags_text = tk.Text(selected_tags_frame, height=3, wrap=tk.WORD)
        self.selected_tags_text.pack(fill=tk.X, expand=True)
        
        # 添加标签复选框的变量
        self.tag_vars = {}
        self.populate_tags()  # 填充标签
        
        # 平台选择
        platform_frame = ttk.Frame(filter_frame)
        platform_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.windows_var = tk.BooleanVar(value=True)
        self.mac_var = tk.BooleanVar(value=False)
        self.linux_var = tk.BooleanVar(value=False)
        
        ttk.Checkbutton(platform_frame, text="Windows", variable=self.windows_var).pack(side=tk.LEFT)
        ttk.Checkbutton(platform_frame, text="Mac", variable=self.mac_var).pack(side=tk.LEFT)
        ttk.Checkbutton(platform_frame, text="Linux", variable=self.linux_var).pack(side=tk.LEFT)
        
        # 搜索按钮
        search_button = ttk.Button(filter_frame, text="搜索游戏", command=self.search_games)
        search_button.pack(fill=tk.X, padx=5, pady=10)
        
        # 重置按钮
        reset_button = ttk.Button(filter_frame, text="重置筛选", command=self.reset_filters)
        reset_button.pack(fill=tk.X, padx=5, pady=5)
        
        # 右侧结果显示区域
        results_frame = ttk.LabelFrame(right_frame, text="搜索结果")
        results_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 创建树形视图来显示游戏数据
        columns = ("name", "release_date", "price", "positive_rate", "owners", "peak_ccu")
        self.results_tree = ttk.Treeview(results_frame, columns=columns, show="headings")
        
        # 定义列
        self.results_tree.heading("name", text="游戏名称")
        self.results_tree.heading("release_date", text="发布日期")
        self.results_tree.heading("price", text="价格")
        self.results_tree.heading("positive_rate", text="好评率")
        self.results_tree.heading("owners", text="拥有者估计")
        self.results_tree.heading("peak_ccu", text="峰值同时在线")
        
        # 列宽
        self.results_tree.column("name", width=250)
        self.results_tree.column("release_date", width=100)
        self.results_tree.column("price", width=80)
        self.results_tree.column("positive_rate", width=80)
        self.results_tree.column("owners", width=150)
        self.results_tree.column("peak_ccu", width=100)
        
        # 添加滚动条
        scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.results_tree.yview)
        self.results_tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.results_tree.pack(fill=tk.BOTH, expand=True)
        
        # 游戏详情区域
        details_frame = ttk.LabelFrame(right_frame, text="游戏详情")
        details_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.details_text = scrolledtext.ScrolledText(details_frame, height=10)
        self.details_text.pack(fill=tk.BOTH, expand=True)
        
        # 绑定事件：当选择一个游戏时显示详情
        self.results_tree.bind("<<TreeviewSelect>>", self.show_game_details)
    
    def reset_filters(self):
        """重置所有筛选选项"""
        self.search_entry.delete(0, tk.END)
        self.min_price_var.set("0")
        self.max_price_var.set("100")
        self.sort_var.set("游玩人数 (高到低)")
        self.windows_var.set(True)
        self.mac_var.set(False)
        self.linux_var.set(False)
        
        # 重置标签选择
        for tag_var in self.tag_vars.values():
            tag_var.set(False)
        
        # 更新已选标签显示
        self.update_selected_tags_display()
        
        # 重新搜索
        self.search_games()
    
    def search_games(self):
        """根据筛选条件搜索游戏"""
        try:
            # 清空当前结果
            for i in self.results_tree.get_children():
                self.results_tree.delete(i)
            
            # 构建MongoDB查询
            query = {}
            
            # 游戏名称搜索
            search_text = self.search_entry.get().strip()
            if search_text:
                query["name"] = {"$regex": search_text, "$options": "i"}
            
            # 价格范围
            try:
                min_price = float(self.min_price_var.get())
                max_price = float(self.max_price_var.get())
                query["price"] = {"$gte": min_price, "$lte": max_price}
            except ValueError:
                pass
            
            # 平台选择 - 修改为同时满足所有选中平台
            platform_conditions = []
            if self.windows_var.get():
                platform_conditions.append({"windows": "True"})
            if self.mac_var.get():
                platform_conditions.append({"mac": "True"})
            if self.linux_var.get():
                platform_conditions.append({"linux": "True"})
            
            if platform_conditions:
                if len(platform_conditions) == 1:
                    query.update(platform_conditions[0])
                else:
                    if "$and" not in query:
                        query["$and"] = platform_conditions
                    else:
                        query["$and"].extend(platform_conditions)
            
            # 标签选择 - 单独处理所有标签
            selected_tags = [tag for tag, var in self.tag_vars.items() if var.get()]
            if selected_tags:
                tag_conditions = []
                for tag in selected_tags:
                    # 使用正则表达式查询标签
                    tag_regex = f"'{re.escape(tag)}':\\s*\\d+"
                    tag_conditions.append({"tags": {"$regex": tag_regex}})
                
                # 确保同时满足所有选中的标签条件
                if "$and" not in query:
                    query["$and"] = tag_conditions
                else:
                    query["$and"].extend(tag_conditions)
            
            # 排序方式
            sort_option = self.sort_var.get()
            sort_field = "peak_ccu"  # 默认按峰值同时在线排序
            sort_order = -1  # -1为降序，1为升序
            
            if sort_option == "发布日期 (新到旧)":
                sort_field = "release_date"
                sort_order = -1
            elif sort_option == "价格 (低到高)":
                sort_field = "price"
                sort_order = 1
            elif sort_option == "评价 (好到差)":
                sort_field = "pct_pos_total"  # 使用正面评价百分比
                sort_order = -1
            elif sort_option == "名称 (A-Z)":
                sort_field = "name"
                sort_order = 1
            
            # 打印查询条件（调试用）
            print("查询条件:", query)
            
            # 执行查询
            results = self.collection.find(query).sort(sort_field, sort_order).limit(100)
            
            # 显示结果
            count = 0
            for game in results:
                # 提取需要的字段
                name = game.get("name", "未知")
                release_date = game.get("release_date", "未知")
                price = f"${game.get('price', 0)}"
                
                # 计算好评率
                positive = game.get("positive", 0)
                negative = game.get("negative", 0)
                if positive + negative > 0:
                    positive_rate = f"{positive / (positive + negative) * 100:.1f}%"
                else:
                    positive_rate = "无评价"
                
                owners = game.get("estimated_owners", "未知")
                
                # 处理峰值同时在线数据
                peak_ccu = game.get("peak_ccu", 0)
                if peak_ccu in [0, 1]:
                    peak_ccu_display = "Data not calculated"
                else:
                    peak_ccu_display = str(peak_ccu)
                
                # 添加到树形视图
                game_id = str(game.get("_id"))
                self.results_tree.insert("", tk.END, values=(name, release_date, price, positive_rate, owners, peak_ccu_display), iid=game_id)
                count += 1
            
            # 更新结果数量
            self.results_tree.heading("#0", text=f"找到 {count} 款游戏")
            
        except Exception as e:
            messagebox.showerror("搜索错误", f"搜索时出错: {e}")
            print("详细错误:", e)
    
    def show_game_details(self, event):
        """显示所选游戏的详细信息"""
        try:
            # 清空当前详情
            self.details_text.delete(1.0, tk.END)
            
            # 获取选中的游戏ID
            selected_items = self.results_tree.selection()
            if not selected_items:
                return
            
            selected_id = selected_items[0]
            print(f"选中的游戏ID: {selected_id}")
            
            # 查询游戏详情 - 需要将字符串ID转换为ObjectId
            try:
                # 尝试转换为ObjectId
                object_id = ObjectId(selected_id)
                game = self.collection.find_one({"_id": object_id})
            except:
                # 如果转换失败，尝试直接使用字符串ID查询
                game = self.collection.find_one({"_id": selected_id})
            
            print(f"查询到的游戏: {game is not None}")
            
            if not game:
                self.details_text.insert(tk.END, "未找到该游戏的详细信息。")
                return
            
            # 格式化详情显示
            details = f"游戏名称: {game.get('name', '未知')}\n"
            details += f"发布日期: {game.get('release_date', '未知')}\n"
            
            # 开发商可能是字符串或列表
            developers = game.get('developers', ['未知'])
            if isinstance(developers, list):
                details += f"开发商: {', '.join(developers)}\n"
            else:
                details += f"开发商: {developers}\n"
            
            # 发行商可能是字符串或列表
            publishers = game.get('publishers', ['未知'])
            if isinstance(publishers, list):
                details += f"发行商: {', '.join(publishers)}\n"
            else:
                details += f"发行商: {publishers}\n"
            
            details += f"价格: ${game.get('price', 0)}\n"
            
            # 评价信息
            positive = game.get("positive", 0)
            negative = game.get("negative", 0)
            if positive + negative > 0:
                positive_rate = f"{positive / (positive + negative) * 100:.1f}%"
                details += f"评价: {positive_rate} 好评 ({positive} 好评 / {negative} 差评)\n"
            else:
                details += "评价: 无评价\n"
            
            details += f"估计拥有者: {game.get('estimated_owners', '未知')}\n"
            
            # 处理峰值同时在线数据
            peak_ccu = game.get("peak_ccu", 0)
            if peak_ccu in [0, 1]:
                details += "峰值同时在线: Data not calculated\n"
            else:
                details += f"峰值同时在线: {peak_ccu}\n"
            
            # 游戏类型和标签
            genres = game.get("genres", [])
            if genres:
                if isinstance(genres, list):
                    details += f"游戏类型: {', '.join(genres)}\n"
                else:
                    details += f"游戏类型: {genres}\n"
            
            # 显示标签信息
            tags = game.get("tags", "")
            if tags and isinstance(tags, str) and tags.startswith("{"):
                try:
                    # 尝试解析标签字符串
                    tag_dict = json.loads(tags.replace("'", "\""))
                    top_tags = sorted(tag_dict.items(), key=lambda x: int(x[1]), reverse=True)[:5]
                    details += f"主要标签: {', '.join([tag for tag, _ in top_tags])}\n"
                except:
                    pass
            
            # 游戏描述
            details += f"\n游戏简介:\n{game.get('short_description', '无描述信息')}\n"
            
            # 显示详情
            self.details_text.insert(tk.END, details)
            
        except Exception as e:
            messagebox.showerror("详情错误", f"显示游戏详情时出错: {e}")
            print("详情错误详细信息:", e)  # 在控制台打印更多信息

    def populate_tags(self):
        """填充标签复选框"""
        # 先清除现有的复选框
        for widget in self.tags_scrollable_frame.winfo_children():
            widget.destroy()
        
        # 获取当前搜索文本
        search_text = self.tag_search_var.get().lower() if hasattr(self, 'tag_search_var') else ""
        
        # 筛选标签
        all_tags = self.all_tags if hasattr(self, 'all_tags') else self.get_all_tags()
        filtered_tags = [tag for tag in all_tags if search_text == "" or search_text in tag.lower()]
        
        # 限制显示数量，防止UI过载
        max_display = 10000  # 允许最多显示10000个标签
        display_tags = filtered_tags[:max_display]
        
        # 创建复选框网格
        cols = 3  # 每行3个复选框
        for i, tag in enumerate(display_tags):
            row, col = i // cols, i % cols
            
            if tag not in self.tag_vars:
                self.tag_vars[tag] = tk.BooleanVar()
                # 只在第一次创建变量时添加追踪
                self.tag_vars[tag].trace_add("write", self.update_selected_tags_display_callback)
            
            chk = ttk.Checkbutton(
                self.tags_scrollable_frame, 
                text=tag,
                variable=self.tag_vars[tag]
            )
            chk.grid(row=row, column=col, sticky="w", padx=5, pady=2)
        
        # 显示筛选信息
        if search_text and len(filtered_tags) > max_display:
            info_label = ttk.Label(
                self.tags_scrollable_frame,
                text=f"显示前{max_display}个结果，共找到{len(filtered_tags)}个标签"
            )
            info_label.grid(row=(len(display_tags)//cols)+1, column=0, columnspan=cols, pady=5)
        
        # 更新已选标签显示
        self.update_selected_tags_display()

    def update_selected_tags_display_callback(self, *args):
        """回调函数，用于变量追踪"""
        self.update_selected_tags_display()

    def filter_tags(self, *args):
        """根据搜索文本筛选标签"""
        self.populate_tags()

    def update_selected_tags_display(self):
        """更新已选标签的显示"""
        selected_tags = [tag for tag, var in self.tag_vars.items() if var.get()]
        
        # 清空当前显示
        self.selected_tags_text.delete(1.0, tk.END)
        
        if selected_tags:
            self.selected_tags_text.insert(tk.END, ", ".join(selected_tags))
        else:
            self.selected_tags_text.insert(tk.END, "未选择任何标签")

if __name__ == "__main__":
    root = tk.Tk()
    app = SteamDatabaseGUI(root)
    root.mainloop()