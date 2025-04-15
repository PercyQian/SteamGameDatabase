import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import pandas as pd
from pymongo import MongoClient
import json
import re
from functools import partial
from bson.objectid import ObjectId  # import this, for handling MongoDB's ObjectId

class SteamDatabaseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Steam游戏数据库查询工具")
        self.root.geometry("1200x800")
        self.root.configure(bg="#2a475e")  # dark blue background
        
        # setup styles
        self.setup_styles()
        
        # connect to MongoDB
        self.connect_to_mongodb()
        
        # get all available tags
        self.all_tags = self.get_all_tags()
        
        # create UI elements
        self.create_ui()
        
        # display initial data
        self.search_games()
    
    def setup_styles(self):
        """set custom styles"""
        style = ttk.Style()
        # create special style for game type checkboxes
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
            print("MongoDB connected successfully")
        except Exception as e:
            messagebox.showerror("Connection error", f"Failed to connect to MongoDB: {e}")
    
    def get_all_tags(self):
        """get all available tags"""
        try:
            # try to get all possible tags from the tags field
            pipeline = [
                {"$match": {"tags": {"$exists": True, "$ne": ""}}},
                {"$limit": 1000}  # limit the number of documents to process for performance
            ]
            
            cursor = self.collection.aggregate(pipeline)
            all_tags = set()
            
            for doc in cursor:
                tags = doc.get("tags", "")
                if isinstance(tags, str) and tags.startswith("{"):
                    try:
                        # parse the tag string into a dictionary
                        tag_dict = json.loads(tags.replace("'", "\""))
                        # add all tags to the set
                        all_tags.update(tag_dict.keys())
                    except:
                        pass
            
            return sorted(list(all_tags))
        except Exception as e:
            print(f"Error getting tags: {e}")
            return []
    
    def get_all_genres(self):
        """get all game types"""
        try:
            genres = self.collection.distinct("genres")
            # if genres is a string array, return it directly
            if genres and isinstance(genres[0], str):
                return sorted(genres)
            
            # if genres is an array field, need to flatten it
            flat_genres = []
            for genre_list in genres:
                if isinstance(genre_list, list):
                    flat_genres.extend(genre_list)
            
            return sorted(list(set(flat_genres)))
        except Exception as e:
            print(f"Error getting game types: {e}")
            return []
    
    def create_ui(self):
        # create main frame
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # split into two parts
        left_frame = ttk.Frame(main_frame, width=400)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, padx=5, pady=5)
        
        right_frame = ttk.Frame(main_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # left filter area
        filter_frame = ttk.LabelFrame(left_frame, text="filter options")
        filter_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # search box
        search_frame = ttk.Frame(filter_frame)
        search_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(search_frame, text="game name:").pack(side=tk.LEFT)
        self.search_entry = ttk.Entry(search_frame)
        self.search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        # price range
        price_frame = ttk.Frame(filter_frame)
        price_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(price_frame, text="price range:").pack(side=tk.LEFT)
        self.min_price_var = tk.StringVar(value="0")
        self.max_price_var = tk.StringVar(value="100")
        
        ttk.Label(price_frame, text="from").pack(side=tk.LEFT)
        ttk.Entry(price_frame, textvariable=self.min_price_var, width=5).pack(side=tk.LEFT)
        ttk.Label(price_frame, text="to").pack(side=tk.LEFT)
        ttk.Entry(price_frame, textvariable=self.max_price_var, width=5).pack(side=tk.LEFT)
        
        # sort options
        sort_frame = ttk.Frame(filter_frame)
        sort_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(sort_frame, text="sort options:").pack(side=tk.LEFT)
        sort_options = ["peak_ccu (high to low)", "release_date (new to old)", "price (low to high)", "positive_rate (good to bad)", "name (A-Z)"]
        self.sort_var = tk.StringVar(value=sort_options[0])
        sort_dropdown = ttk.Combobox(sort_frame, textvariable=self.sort_var, values=sort_options, state="readonly", width=15)
        sort_dropdown.pack(side=tk.LEFT, padx=5)
        
        # tag selection (multiple checkboxes)
        tags_frame = ttk.LabelFrame(filter_frame, text="game tags (multi-select)")
        tags_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # add tag search box
        tag_search_frame = ttk.Frame(tags_frame)
        tag_search_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(tag_search_frame, text="search tags:").pack(side=tk.LEFT)
        self.tag_search_var = tk.StringVar()
        self.tag_search_var.trace("w", self.filter_tags)
        tag_search_entry = ttk.Entry(tag_search_frame, textvariable=self.tag_search_var)
        tag_search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        # create tag scroll area (with horizontal and vertical scrollbars)
        tags_outer_frame = ttk.Frame(tags_frame)
        tags_outer_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # vertical scrollbar
        tags_canvas = tk.Canvas(tags_outer_frame, height=200)
        tags_v_scrollbar = ttk.Scrollbar(tags_outer_frame, orient="vertical", command=tags_canvas.yview)
        # horizontal scrollbar
        tags_h_scrollbar = ttk.Scrollbar(tags_outer_frame, orient="horizontal", command=tags_canvas.xview)
        
        self.tags_scrollable_frame = ttk.Frame(tags_canvas)
        
        # configure scroll area
        self.tags_scrollable_frame.bind(
            "<Configure>",
            lambda e: tags_canvas.configure(scrollregion=tags_canvas.bbox("all"))
        )
        
        # create window and configure scrollbars
        tags_canvas.create_window((0, 0), window=self.tags_scrollable_frame, anchor="nw")
        tags_canvas.configure(yscrollcommand=tags_v_scrollbar.set, xscrollcommand=tags_h_scrollbar.set)
        
        # layout scroll area components
        tags_v_scrollbar.pack(side="right", fill="y")
        tags_h_scrollbar.pack(side="bottom", fill="x")
        tags_canvas.pack(side="left", fill="both", expand=True)
        
        # add selected tags display area
        selected_tags_frame = ttk.LabelFrame(tags_frame, text="selected tags")
        selected_tags_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.selected_tags_text = tk.Text(selected_tags_frame, height=3, wrap=tk.WORD)
        self.selected_tags_text.pack(fill=tk.X, expand=True)
        
        # add variable for tag checkboxes
        self.tag_vars = {}
        self.populate_tags()  # fill tags
        
        # platform selection
        platform_frame = ttk.Frame(filter_frame)
        platform_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.windows_var = tk.BooleanVar(value=True)
        self.mac_var = tk.BooleanVar(value=False)
        self.linux_var = tk.BooleanVar(value=False)
        
        ttk.Checkbutton(platform_frame, text="Windows", variable=self.windows_var).pack(side=tk.LEFT)
        ttk.Checkbutton(platform_frame, text="Mac", variable=self.mac_var).pack(side=tk.LEFT)
        ttk.Checkbutton(platform_frame, text="Linux", variable=self.linux_var).pack(side=tk.LEFT)
        
        # search button
        search_button = ttk.Button(filter_frame, text="search games", command=self.search_games)
        search_button.pack(fill=tk.X, padx=5, pady=10)
        
        # reset button
        reset_button = ttk.Button(filter_frame, text="reset filters", command=self.reset_filters)
        reset_button.pack(fill=tk.X, padx=5, pady=5)
        
        # right results display area
        results_frame = ttk.LabelFrame(right_frame, text="search results")
        results_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # create treeview to display game data
        columns = ("name", "release_date", "price", "positive_rate", "owners", "peak_ccu")
        self.results_tree = ttk.Treeview(results_frame, columns=columns, show="headings")
        
        # define columns
        self.results_tree.heading("name", text="game name")
        self.results_tree.heading("release_date", text="release date")
        self.results_tree.heading("price", text="price")
        self.results_tree.heading("positive_rate", text="positive rate")
        self.results_tree.heading("owners", text="estimated owners")
        self.results_tree.heading("peak_ccu", text="peak ccu")
        
        # column width
        self.results_tree.column("name", width=250)
        self.results_tree.column("release_date", width=100)
        self.results_tree.column("price", width=80)
        self.results_tree.column("positive_rate", width=80)
        self.results_tree.column("owners", width=150)
        self.results_tree.column("peak_ccu", width=100)
        
        # add scrollbar
        scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=self.results_tree.yview)
        self.results_tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.results_tree.pack(fill=tk.BOTH, expand=True)
        
        # game details area
        details_frame = ttk.LabelFrame(right_frame, text="game details")
        details_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.details_text = scrolledtext.ScrolledText(details_frame, height=10)
        self.details_text.pack(fill=tk.BOTH, expand=True)
        
        # bind event: when a game is selected, show details
        self.results_tree.bind("<<TreeviewSelect>>", self.show_game_details)
    
    def reset_filters(self):
        """reset all filter options"""
        self.search_entry.delete(0, tk.END)
        self.min_price_var.set("0")
        self.max_price_var.set("100")
        self.sort_var.set("peak_ccu (high to low)")
        self.windows_var.set(True)
        self.mac_var.set(False)
        self.linux_var.set(False)
        
        # reset tag selection
        for tag_var in self.tag_vars.values():
            tag_var.set(False)
        
        # update selected tags display
        self.update_selected_tags_display()
        
        # search again
        self.search_games()
    
    def search_games(self):
        """search games based on filter conditions"""
        try:
            # clear current results
            for i in self.results_tree.get_children():
                self.results_tree.delete(i)
            
            # build MongoDB query
            query = {}
            
            # game name search
            search_text = self.search_entry.get().strip()
            if search_text:
                query["name"] = {"$regex": search_text, "$options": "i"}
            
            # price range
            try:
                min_price = float(self.min_price_var.get())
                max_price = float(self.max_price_var.get())
                query["price"] = {"$gte": min_price, "$lte": max_price}
            except ValueError:
                pass
            
            # platform selection - modified to satisfy all selected platforms
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
            
            # tag selection - handle all tags separately
            selected_tags = [tag for tag, var in self.tag_vars.items() if var.get()]
            if selected_tags:
                tag_conditions = []
                for tag in selected_tags:
                    # use regex to query tags
                    tag_regex = f"'{re.escape(tag)}':\\s*\\d+"
                    tag_conditions.append({"tags": {"$regex": tag_regex}})
                
                # ensure all selected tag conditions are met
                if "$and" not in query:
                    query["$and"] = tag_conditions
                else:
                    query["$and"].extend(tag_conditions)
            
            # sort options
            sort_option = self.sort_var.get()
            sort_field = "peak_ccu"  # default to sort by peak ccu
            sort_order = -1  # -1 for descending, 1 for ascending
            
            if sort_option == "release_date (new to old)":
                sort_field = "release_date"
                sort_order = -1
            elif sort_option == "price (low to high)":
                sort_field = "price"
                sort_order = 1
            elif sort_option == "positive_rate (good to bad)":
                sort_field = "pct_pos_total"  # use positive rate
                sort_order = -1
            elif sort_option == "name (A-Z)":
                sort_field = "name"
                sort_order = 1
            
            # print query conditions (for debugging)
            print("query conditions:", query)
            
            # execute query
            results = self.collection.find(query).sort(sort_field, sort_order).limit(100)
            
            # display results
            count = 0
            for game in results:
                # extract required fields
                name = game.get("name", "unknown")
                release_date = game.get("release_date", "unknown")
                price = f"${game.get('price', 0)}"
                
                # calculate positive rate
                positive = game.get("positive", 0)
                negative = game.get("negative", 0)
                if positive + negative > 0:
                    positive_rate = f"{positive / (positive + negative) * 100:.1f}%"
                else:
                    positive_rate = "no evaluation"
                
                owners = game.get("estimated_owners", "unknown")
                
                # handle peak ccu data
                peak_ccu = game.get("peak_ccu", 0)
                if peak_ccu in [0, 1]:
                    peak_ccu_display = "Data not calculated"
                else:
                    peak_ccu_display = str(peak_ccu)
                
                # add to treeview
                game_id = str(game.get("_id"))
                self.results_tree.insert("", tk.END, values=(name, release_date, price, positive_rate, owners, peak_ccu_display), iid=game_id)
                count += 1
            
            # update result count
            self.results_tree.heading("#0", text=f"found {count} games")
            
        except Exception as e:
            messagebox.showerror("search error", f"error searching: {e}")
            print("detailed error:", e)
    
    def show_game_details(self, event):
        """show selected game details"""
        try:
            # clear current details
            self.details_text.delete(1.0, tk.END)
            
            # get selected game ID
            selected_items = self.results_tree.selection()
            if not selected_items:
                return
            
            selected_id = selected_items[0]
            print(f"selected game ID: {selected_id}")
            
            # query game details - need to convert string ID to ObjectId
            try:
                # try to convert to ObjectId
                object_id = ObjectId(selected_id)
                game = self.collection.find_one({"_id": object_id})
            except:
                # if conversion fails, try to query directly using string ID
                game = self.collection.find_one({"_id": selected_id})
            
            print(f"found game: {game is not None}")
            
            if not game:
                self.details_text.insert(tk.END, "no details found.")
                return
            
            # format details display
            details = f"game name: {game.get('name', 'unknown')}\n"
            details += f"release date: {game.get('release_date', 'unknown')}\n"
            
            # developers can be a string or a list
            developers = game.get('developers', ['unknown'])
            if isinstance(developers, list):
                details += f"developers: {', '.join(developers)}\n"
            else:
                details += f"developers: {developers}\n"
            
            # publishers can be a string or a list
            publishers = game.get('publishers', ['unknown'])
            if isinstance(publishers, list):
                details += f"publishers: {', '.join(publishers)}\n"
            else:
                details += f"publishers: {publishers}\n"
            
            details += f"price: ${game.get('price', 0)}\n"
            
            # evaluation information
            positive = game.get("positive", 0)
            negative = game.get("negative", 0)
            if positive + negative > 0:
                positive_rate = f"{positive / (positive + negative) * 100:.1f}%"
                details += f"evaluation: {positive_rate} positive ({positive} positive / {negative} negative)\n"
            else:
                details += "evaluation: no evaluation\n"
            
            details += f"estimated owners: {game.get('estimated_owners', 'unknown')}\n"
            
            # handle peak ccu data
            peak_ccu = game.get("peak_ccu", 0)
            if peak_ccu in [0, 1]:
                details += "peak ccu: Data not calculated\n"
            else:
                details += f"peak ccu: {peak_ccu}\n"
            
            # game type and tags
            genres = game.get("genres", [])
            if genres:
                if isinstance(genres, list):
                    details += f"game type: {', '.join(genres)}\n"
                else:
                    details += f"game type: {genres}\n"
            
            # show tag information
            tags = game.get("tags", "")
            if tags and isinstance(tags, str) and tags.startswith("{"):
                try:
                    # try to parse tag string
                    tag_dict = json.loads(tags.replace("'", "\""))
                    top_tags = sorted(tag_dict.items(), key=lambda x: int(x[1]), reverse=True)[:5]
                    details += f"main tags: {', '.join([tag for tag, _ in top_tags])}\n"
                except:
                    pass
            
            # game description
            details += f"\ngame description:\n{game.get('short_description', 'no description')}\n"
            
            # show details
            self.details_text.insert(tk.END, details)
            
        except Exception as e:
            messagebox.showerror("details error", f"error showing game details: {e}")
            print("details error details:", e)  # print more information on the console

    def populate_tags(self):
        """fill tag checkboxes"""
        # first clear existing checkboxes
        for widget in self.tags_scrollable_frame.winfo_children():
            widget.destroy()
        
        # get current search text
        search_text = self.tag_search_var.get().lower() if hasattr(self, 'tag_search_var') else ""
        
        # filter tags
        all_tags = self.all_tags if hasattr(self, 'all_tags') else self.get_all_tags()
        filtered_tags = [tag for tag in all_tags if search_text == "" or search_text in tag.lower()]
        
        # limit display count, prevent UI overload
        max_display = 1000  # allow up to 1000 tags to be displayed
        display_tags = filtered_tags[:max_display]
        
        # create checkbox grid
        cols = 3  # 3 checkboxes per row
        for i, tag in enumerate(display_tags):
            row, col = i // cols, i % cols
            
            if tag not in self.tag_vars:
                self.tag_vars[tag] = tk.BooleanVar()
                # only add tracking when the variable is first created
                self.tag_vars[tag].trace_add("write", self.update_selected_tags_display_callback)
            
            chk = ttk.Checkbutton(
                self.tags_scrollable_frame, 
                text=tag,
                variable=self.tag_vars[tag]
            )
            chk.grid(row=row, column=col, sticky="w", padx=5, pady=2)
        
        # show filter information
        if search_text and len(filtered_tags) > max_display:
            info_label = ttk.Label(
                self.tags_scrollable_frame,
                text=f"showing first {max_display} results, found {len(filtered_tags)} tags"
            )
            info_label.grid(row=(len(display_tags)//cols)+1, column=0, columnspan=cols, pady=5)
        
        # update selected tags display
        self.update_selected_tags_display()

    def update_selected_tags_display_callback(self, *args):
        """callback function for variable tracking"""
        self.update_selected_tags_display()

    def filter_tags(self, *args):
        """filter tags based on search text"""
        self.populate_tags()

    def update_selected_tags_display(self):
        """update selected tags display"""
        selected_tags = [tag for tag, var in self.tag_vars.items() if var.get()]
        
        # clear current display
        self.selected_tags_text.delete(1.0, tk.END)
        
        if selected_tags:
            self.selected_tags_text.insert(tk.END, ", ".join(selected_tags))
        else:
            self.selected_tags_text.insert(tk.END, "no tags selected")

if __name__ == "__main__":
    root = tk.Tk()
    app = SteamDatabaseGUI(root)
    root.mainloop()