import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import threading
import re
import time
import random
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
import sys
import json

# ---------- RESOURCE PATH (FOR PYINSTALLER) ----------

def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)


# ---------- SPLASH SCREEN ----------

def show_splash(root):
    splash = tk.Toplevel(root)
    splash.overrideredirect(True)
    splash.configure(bg="#1E1E1E")

    width = 500
    height = 250

    screen_w = splash.winfo_screenwidth()
    screen_h = splash.winfo_screenheight()

    x = int((screen_w/2)-(width/2))
    y = int((screen_h/2)-(height/2))

    splash.geometry(f"{width}x{height}+{x}+{y}")

    frame = tk.Frame(splash, bg="#1E1E1E")
    frame.pack(expand=True)

    title = tk.Label(
        frame,
        text="LPN Conversion Publisher",
        font=("Segoe UI",24,"bold"),
        bg="#1E1E1E",
        fg="#4FC3F7"
    )
    title.pack(pady=10)

    subtitle = tk.Label(
        frame,
        text="Bulk LPN Conversion Utility",
        font=("Segoe UI",12),
        bg="#1E1E1E",
        fg="#BBBBBB"
    )
    subtitle.pack()

    version = tk.Label(
        frame,
        text="Version 1.0",
        font=("Segoe UI",9),
        bg="#1E1E1E",
        fg="#888888"
    )
    version.pack(pady=10)

    splash.update()
    splash.after(2000, splash.destroy)


# ---------- MAIN APP ----------

class BulkLPNUI:

    def __init__(self, root):
        self.root = root
        self.root.title("LPN Conversion Publisher")

        width = 1150
        height = 750

        screen_w = root.winfo_screenwidth()
        screen_h = root.winfo_screenheight()

        x = int((screen_w/2)-(width/2))
        y = int((screen_h/2)-(height/2))

        root.geometry(f"{width}x{height}+{x}+{y}")
        root.configure(bg="#1E1E1E")

        # Variables
        self.env_var = tk.StringVar()
        self.topic_var = tk.StringVar()
        self.dc_var = tk.StringVar()

        # LPN Generation configuration
        self.start_lpn_var = tk.StringVar()
        self.start_parent_lpn_var = tk.StringVar()
        self.location_var = tk.StringVar()
        self.indicator_var = tk.StringVar()
        self.status_var = tk.StringVar()
        self.single_sku_var = tk.StringVar()
        
        # Messages count and batch settings
        self.qty_var = tk.StringVar()
        self.batch_var = tk.StringVar()

        self.progress_var = tk.DoubleVar()
        self.lpn_items = []

        self.apply_styles()
        self.build_ui()

        # Load config to restore previous values (adds LPN items to UI)
        self.load_config()

        # Register traces after loading to avoid overwriting restored topic
        self.env_var.trace_add("write", self.update_topic)
        self.dc_var.trace_add("write", self.update_topic)

        # Save config when window closes
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def on_close(self):
        self.save_config()
        self.root.destroy()

    def save_config(self):
        data = {
            "env": self.env_var.get(),
            "topic": self.topic_var.get(),
            "dc": self.dc_var.get(),
            "start_lpn": self.start_lpn_var.get(),
            "start_parent_lpn": self.start_parent_lpn_var.get(),
            "location": self.location_var.get(),
            "indicator": self.indicator_var.get(),
            "status": self.status_var.get(),
            "single_sku": self.single_sku_var.get(),
            "qty": self.qty_var.get(),
            "batch": self.batch_var.get(),
            "items": []
        }
        for item in self.lpn_items:
            data["items"].append({
                "item": item[6].get(),
                "status": item[7].get(),
                "pack_qty": item[8].get(),
                "actual_qty": item[9].get(),
                "shipped_qty": item[10].get(),
                "lock": item[11].get()
            })
        try:
            config_path = resource_path("lpn_publisher_config.json")
            with open(config_path, "w") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"Error saving config: {e}")

    def load_config(self):
        config_path = resource_path("lpn_publisher_config.json")
        if not os.path.exists(config_path):
            # Default fallback values
            self.env_var.set("EQA2")
            self.dc_var.set("OB1")
            self.start_lpn_var.set("LPN90903362")
            self.start_parent_lpn_var.set("WSAA3105725")
            self.location_var.set("N814010F")
            self.indicator_var.set("I")
            self.status_var.set("30")
            self.single_sku_var.set("Y")
            self.qty_var.set("10")
            self.batch_var.set("100")
            
            self.add_lpn_item()
            self.lpn_items[0][6].set("7232345")
            self.lpn_items[0][7].set("RTL")
            self.lpn_items[0][8].set("1")
            self.lpn_items[0][9].set("4")
            self.lpn_items[0][10].set("4")
            self.lpn_items[0][11].set("OT")
            return

        try:
            with open(config_path, "r") as f:
                data = json.load(f)
            
            self.env_var.set(data.get("env", ""))
            self.topic_var.set(data.get("topic", ""))
            self.dc_var.set(data.get("dc", "OB1"))
            self.start_lpn_var.set(data.get("start_lpn", "LPN90903362"))
            self.start_parent_lpn_var.set(data.get("start_parent_lpn", "WSAA3105725"))
            self.location_var.set(data.get("location", "N814010F"))
            self.indicator_var.set(data.get("indicator", "I"))
            self.status_var.set(data.get("status", "30"))
            self.single_sku_var.set(data.get("single_sku", "Y"))
            self.qty_var.set(data.get("qty", "10"))
            self.batch_var.set(data.get("batch", "100"))

            items = data.get("items", [])
            if not items:
                self.add_lpn_item()
                self.lpn_items[0][6].set("7232345")
                self.lpn_items[0][7].set("RTL")
                self.lpn_items[0][8].set("1")
                self.lpn_items[0][9].set("4")
                self.lpn_items[0][10].set("4")
                self.lpn_items[0][11].set("OT")
            else:
                for idx, val in enumerate(items):
                    self.add_lpn_item()
                    self.lpn_items[idx][6].set(val.get("item", ""))
                    self.lpn_items[idx][7].set(val.get("status", ""))
                    self.lpn_items[idx][8].set(val.get("pack_qty", ""))
                    self.lpn_items[idx][9].set(val.get("actual_qty", ""))
                    self.lpn_items[idx][10].set(val.get("shipped_qty", ""))
                    self.lpn_items[idx][11].set(val.get("lock", ""))
        except Exception as e:
            print(f"Error loading config: {e}")
            if not self.lpn_items:
                self.add_lpn_item()

    # ---------- STYLES ----------

    def apply_styles(self):
        style = ttk.Style()
        style.theme_use("clam")

        style.configure(".", background="#1E1E1E", foreground="white", font=("Segoe UI",10))
        style.configure("TFrame", background="#1E1E1E")
        style.configure("TLabelframe", background="#252526", foreground="white")
        style.configure("TLabelframe.Label", background="#252526", foreground="white")

        style.configure("TLabel", background="#1E1E1E", foreground="white")
        style.configure("TEntry", fieldbackground="#333333", foreground="white")

        style.configure(
            "TButton",
            background="#0E639C",
            foreground="white",
            padding=6,
            font=("Segoe UI",10,"bold")
        )

    # ---------- UI ----------

    def build_ui(self):
        title = tk.Label(
            self.root,
            text="LPN Conversion Publisher",
            font=("Segoe UI",22,"bold"),
            bg="#1E1E1E",
            fg="#4FC3F7"
        )
        title.pack(pady=(10,0))

        subtitle = tk.Label(
            self.root,
            text="Bulk License Plate Number (LPN) Conversion Publisher",
            font=("Segoe UI",11),
            bg="#1E1E1E",
            fg="#BBBBBB"
        )
        subtitle.pack(pady=(0,10))

        main = ttk.Frame(self.root)
        main.pack(fill=tk.BOTH, expand=True, padx=15)

        # Environment Frame
        env_frame = ttk.LabelFrame(main, text="Environment Settings")
        env_frame.pack(fill=tk.X, pady=5)

        ttk.Label(env_frame, text="Environment").grid(row=0, column=0, padx=5, pady=5)
        env_combo = ttk.Combobox(
            env_frame,
            textvariable=self.env_var,
            values=["EQA1", "EQA2", "EQA3"],
            width=10
        )
        env_combo.grid(row=0, column=1)

        ttk.Label(env_frame, text="Topic").grid(row=0, column=2, padx=10)
        ttk.Entry(
            env_frame,
            textvariable=self.topic_var,
            width=45
        ).grid(row=0, column=3)

        ttk.Button(
            env_frame,
            text="Fetch Topics",
            command=self.fetch_topics
        ).grid(row=0, column=4, padx=5)

        # LPN Configuration Frame
        config = ttk.LabelFrame(main, text="LPN Settings")
        config.pack(fill=tk.X, pady=5)

        ttk.Label(config, text="DC").grid(row=0, column=0, padx=5, pady=5)
        ttk.Entry(config, textvariable=self.dc_var, width=10).grid(row=0, column=1, padx=5)

        ttk.Label(config, text="Start LPN").grid(row=0, column=2, padx=5)
        ttk.Entry(config, textvariable=self.start_lpn_var, width=18).grid(row=0, column=3, padx=5)

        ttk.Label(config, text="Start Parent LPN").grid(row=0, column=4, padx=5)
        ttk.Entry(config, textvariable=self.start_parent_lpn_var, width=18).grid(row=0, column=5, padx=5)

        ttk.Label(config, text="Location").grid(row=0, column=6, padx=5)
        ttk.Entry(config, textvariable=self.location_var, width=12).grid(row=0, column=7, padx=5)

        ttk.Label(config, text="Indicator").grid(row=1, column=0, padx=5, pady=5)
        ttk.Entry(config, textvariable=self.indicator_var, width=10).grid(row=1, column=1, padx=5)

        ttk.Label(config, text="Status").grid(row=1, column=2, padx=5)
        ttk.Entry(config, textvariable=self.status_var, width=18).grid(row=1, column=3, padx=5)

        ttk.Label(config, text="Single SKU").grid(row=1, column=4, padx=5)
        ttk.Entry(config, textvariable=self.single_sku_var, width=18).grid(row=1, column=5, padx=5)

        ttk.Label(config, text="Messages").grid(row=1, column=6, padx=5)
        ttk.Entry(config, textvariable=self.qty_var, width=12).grid(row=1, column=7, padx=5)

        # Items details container
        container = ttk.LabelFrame(main, text="LPN Item & Lock Details")
        container.pack(fill=tk.BOTH, pady=10, expand=True)

        canvas = tk.Canvas(container, height=180, bg="#252526", highlightthickness=0)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)

        self.item_frame = ttk.Frame(canvas)
        self.item_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0,0), window=self.item_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        ttk.Button(
            self.item_frame,
            text="+ Add LPN Item Details",
            command=self.add_lpn_item
        ).grid(row=0, column=0, columnspan=2, pady=5, padx=5)

        ttk.Label(self.item_frame, text="Item").grid(row=1, column=1, padx=5)
        ttk.Label(self.item_frame, text="Product Status").grid(row=1, column=2, padx=5)
        ttk.Label(self.item_frame, text="Pack Qty").grid(row=1, column=3, padx=5)
        ttk.Label(self.item_frame, text="Actual Qty").grid(row=1, column=4, padx=5)
        ttk.Label(self.item_frame, text="Shipped Qty").grid(row=1, column=5, padx=5)
        ttk.Label(self.item_frame, text="Lock Code").grid(row=1, column=6, padx=5)

        # Items are loaded and initialized via load_config() on startup

        # Action Buttons
        actions = ttk.Frame(main)
        actions.pack(pady=10)

        ttk.Button(
            actions,
            text="Preview Range",
            command=self.preview_range
        ).pack(side=tk.LEFT, padx=10)

        ttk.Button(
            actions,
            text="START BULK PUBLISH",
            command=self.start_thread
        ).pack(side=tk.LEFT, padx=10)

        # Progress bar
        ttk.Label(main, text="Publishing Progress").pack()
        self.progress = ttk.Progressbar(
            main,
            variable=self.progress_var,
            maximum=100,
            length=500
        )
        self.progress.pack(pady=5)

        # Console
        log_frame = ttk.LabelFrame(main, text="Console")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=10)

        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            font=("Consolas",10),
            bg="#111111",
            fg="#00FF9C",
            insertbackground="white"
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)

    # ---------- TOPIC AUTO SET ----------

    def update_topic(self, *args):
        env = self.env_var.get()
        dc = self.dc_var.get()
        if not env or not dc:
            return

        topic = f"{env}.L.ROUTING.TXN.{dc}"
        self.topic_var.set(topic)

    # ---------- KAFKA CONFIG ----------

    def get_kafka_config(self):
        cert_path = resource_path("kafka_truststore.pem")
        if not os.path.exists(cert_path):
            cert_path = resource_path("kafka_cert.pem")
        return (
            "rk.qa.kafka.logistics.wsgc.com:443",
            "appwmos",
            "n3w0rk",
            cert_path
        )

    # ---------- FETCH TOPICS ----------

    def fetch_topics(self):
        env = self.env_var.get()
        if not env:
            messagebox.showerror("Error", "Select Environment first")
            return

        try:
            broker, user, pwd, cert = self.get_kafka_config()
            admin = KafkaAdminClient(
                bootstrap_servers=broker,
                security_protocol="SASL_SSL",
                sasl_mechanism="SCRAM-SHA-512",
                sasl_plain_username=user,
                sasl_plain_password=pwd,
                ssl_cafile=cert,
                api_version=(2, 0, 0)
            )
            topics = admin.list_topics()
            env_num = env[3:]
            env_topics = [t for t in topics if f"QA{env_num}" in t]

            if not env_topics:
                messagebox.showinfo("Topics", "No topics found")
                return

            self.show_topic_selector(env_topics)
        except Exception as e:
            messagebox.showerror("Kafka Error", str(e))

    # ---------- TOPIC SELECTOR WITH SEARCH ----------

    def show_topic_selector(self, topics):
        win = tk.Toplevel(self.root)
        win.title("Select Kafka Topic")
        win.geometry("550x450")

        container = ttk.Frame(win)
        container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        ttk.Label(container, text="Search").pack(anchor="w")
        search_var = tk.StringVar()
        search_entry = ttk.Entry(container, textvariable=search_var)
        search_entry.pack(fill=tk.X, pady=5)

        listbox = tk.Listbox(container, font=("Consolas",10))
        listbox.pack(fill=tk.BOTH, expand=True)

        topics_sorted = sorted(topics)

        def populate_list(data):
            listbox.delete(0, tk.END)
            for t in data:
                listbox.insert(tk.END, t)

        populate_list(topics_sorted)

        def filter_topics(*args):
            search = search_var.get().lower()
            filtered = [t for t in topics_sorted if search in t.lower()]
            populate_list(filtered)

        search_var.trace_add("write", filter_topics)

        def select_topic():
            if not listbox.curselection():
                return
            selected = listbox.get(listbox.curselection())
            self.topic_var.set(selected)
            win.destroy()

        ttk.Button(container, text="Use Selected Topic", command=select_topic).pack(pady=10)
        search_entry.focus()

    # ---------- LPN ITEMS MANAGEMENT ----------

    def add_lpn_item(self):
        row = len(self.lpn_items) + 2

        item_var = tk.StringVar()
        status_var = tk.StringVar(value="RTL")
        pack_qty_var = tk.StringVar(value="1")
        actual_qty_var = tk.StringVar(value="4")
        shipped_qty_var = tk.StringVar(value="4")
        lock_var = tk.StringVar(value="OT")

        e_item = ttk.Entry(self.item_frame, textvariable=item_var, width=15)
        e_item.grid(row=row, column=1, padx=2, pady=2)

        e_status = ttk.Entry(self.item_frame, textvariable=status_var, width=12)
        e_status.grid(row=row, column=2, padx=2, pady=2)

        e_pack = ttk.Entry(self.item_frame, textvariable=pack_qty_var, width=10)
        e_pack.grid(row=row, column=3, padx=2, pady=2)

        e_actual = ttk.Entry(self.item_frame, textvariable=actual_qty_var, width=10)
        e_actual.grid(row=row, column=4, padx=2, pady=2)

        e_shipped = ttk.Entry(self.item_frame, textvariable=shipped_qty_var, width=10)
        e_shipped.grid(row=row, column=5, padx=2, pady=2)

        e_lock = ttk.Entry(self.item_frame, textvariable=lock_var, width=10)
        e_lock.grid(row=row, column=6, padx=2, pady=2)

        btn_delete = ttk.Button(
            self.item_frame,
            text="Delete",
            command=lambda r=row: self.delete_lpn_item(r)
        )
        btn_delete.grid(row=row, column=7, padx=5, pady=2)

        self.lpn_items.append((e_item, e_status, e_pack, e_actual, e_shipped, e_lock, item_var, status_var, pack_qty_var, actual_qty_var, shipped_qty_var, lock_var, btn_delete))

    def delete_lpn_item(self, row):
        index = row - 2
        if index >= len(self.lpn_items) or index < 0:
            return

        widgets = [
            self.lpn_items[index][0], self.lpn_items[index][1], self.lpn_items[index][2], 
            self.lpn_items[index][3], self.lpn_items[index][4], self.lpn_items[index][5],
            self.lpn_items[index][12]
        ]
        for w in widgets:
            w.destroy()

        self.lpn_items.pop(index)
        self.refresh_items()

    def refresh_items(self):
        for i, item in enumerate(self.lpn_items, start=2):
            e_item, e_status, e_pack, e_actual, e_shipped, e_lock, _, _, _, _, _, _, btn_delete = item
            e_item.grid(row=i, column=1)
            e_status.grid(row=i, column=2)
            e_pack.grid(row=i, column=3)
            e_actual.grid(row=i, column=4)
            e_shipped.grid(row=i, column=5)
            e_lock.grid(row=i, column=6)
            btn_delete.config(command=lambda r=i: self.delete_lpn_item(r))
            btn_delete.grid(row=i, column=7)

    # ---------- LOG ----------

    def log(self, msg):
        t = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{t}] {msg}\n")
        self.log_text.see(tk.END)
        self.root.update()

    # ---------- XML PRETTY PRINT ----------

    def pretty_xml(self, xml_string):
        try:
            dom = minidom.parseString(xml_string)
            return dom.toprettyxml(indent="  ")
        except:
            return xml_string

    # ---------- XML PREVIEW ----------

    def preview_xml_window(self, xml):
        preview = tk.Toplevel(self.root)
        preview.title("Preview Generated LPN Conversion XML")
        preview.geometry("900x600")

        text = scrolledtext.ScrolledText(preview, font=("Consolas",10))
        text.pack(fill=tk.BOTH, expand=True)
        text.insert("1.0", self.pretty_xml(xml))

        result = {"confirm": False, "xml": xml}

        def confirm():
            edited = text.get("1.0", tk.END).strip()
            result["confirm"] = True
            result["xml"] = edited
            preview.destroy()

        ttk.Button(preview, text="Confirm & Send", command=confirm).pack(pady=10)
        preview.grab_set()
        self.root.wait_window(preview)
        return result

    # ---------- SEQUENCE GENERATOR ----------

    def generate_sequence(self, start, count):
        if not start:
            return [""] * count
        match = re.match(r"(.*?)(\d+)$", start)
        if not match:
            return [start] * count
        prefix = match.group(1)
        number = int(match.group(2))
        padding = len(match.group(2))

        seq = []
        for i in range(count):
            seq.append(prefix + str(number + i).zfill(padding))
        return seq

    # ---------- PREVIEW RANGE ----------

    def preview_range(self):
        start_lpn = self.start_lpn_var.get()
        start_parent = self.start_parent_lpn_var.get()
        qty = int(self.qty_var.get())

        lpn_seq = self.generate_sequence(start_lpn, qty)
        parent_val = start_parent if start_parent else "(Blank)"

        messagebox.showinfo(
            "LPN Range Preview",
            f"Start LPN: {lpn_seq[0]}\nEnd LPN: {lpn_seq[-1]}\n"
            f"Parent LPN (Pallet): {parent_val} (Fixed)\nTotal Messages: {qty}"
        )

    # ---------- XML GENERATOR ----------

    def generate_xml(self, lpn_val, parent_lpn_val):
        dc = self.dc_var.get()
        
        # Create tXML Root
        txml = ET.Element("tXML")
        
        # Header Element
        header = ET.SubElement(txml, "Header")
        ET.SubElement(header, "Source").text = "Host"
        ET.SubElement(header, "Action_Type").text = "Update"
        
        # Reference ID: LPNR + 8 random digits
        ref_id = f"LPNR{random.randint(10000000, 99999999)}"
        ET.SubElement(header, "Reference_ID").text = ref_id
        
        ET.SubElement(header, "Message_Type").text = "LPNConversion"
        ET.SubElement(header, "Company_ID").text = "1"
        ET.SubElement(header, "Version").text = "2020"
        ET.SubElement(header, "Internal_Reference_ID").text = dc
        ET.SubElement(header, "Internal_Date_Time_Stamp").text = datetime.now().strftime("%m/%d/%Y %H:%M")

        # Message Element
        message = ET.SubElement(txml, "Message")
        
        # LPN parsing starting value
        lpn_seq = self.generate_sequence(lpn_val, len(self.lpn_items))
        parent_seq = [parent_lpn_val] * len(self.lpn_items) if parent_lpn_val else [""] * len(self.lpn_items)

        # Add one Lpn node for each detail row in the list
        for idx, item in enumerate(self.lpn_items):
            item_name = item[6].get()
            prod_status = item[7].get()
            pack_qty = item[8].get()
            act_qty = item[9].get()
            ship_qty = item[10].get()
            lock_code = item[11].get()
            
            # Keep TCCompanyID at 1
            comp_id_seq = "1"

            lpn = ET.SubElement(message, "Lpn")
            ET.SubElement(lpn, "FacilityAliasID").text = dc
            ET.SubElement(lpn, "TCCompanyID").text = comp_id_seq
            ET.SubElement(lpn, "TCLpnID").text = lpn_seq[idx]
            
            if parent_seq[idx]:
                ET.SubElement(lpn, "ParentTCLpnID").text = parent_seq[idx]
            else:
                ET.SubElement(lpn, "ParentTCLpnID") # Empty element
                
            ET.SubElement(lpn, "Location").text = self.location_var.get()
            ET.SubElement(lpn, "InboundOutboundIndicator").text = self.indicator_var.get()
            ET.SubElement(lpn, "LpnFacilityStatus").text = self.status_var.get()
            ET.SubElement(lpn, "LpnSizeType")
            ET.SubElement(lpn, "SingleSKULpn").text = self.single_sku_var.get()
            ET.SubElement(lpn, "ReceivedDate").text = datetime.now().strftime("%m/%d/%Y 00:00")

            # LpnDetail Element
            lpn_detail = ET.SubElement(lpn, "LpnDetail")
            ET.SubElement(lpn_detail, "TCCompanyID").text = comp_id_seq
            ET.SubElement(lpn_detail, "Item").text = item_name
            ET.SubElement(lpn_detail, "ProductStatus").text = prod_status
            ET.SubElement(lpn_detail, "StandardPackQuantity").text = pack_qty
            ET.SubElement(lpn_detail, "ActualQuantity").text = act_qty
            ET.SubElement(lpn_detail, "ShippedQuantity").text = ship_qty

            # LPNLock Element (optional, only if lock code is provided)
            if lock_code:
                lpn_lock = ET.SubElement(lpn, "LPNLock")
                ET.SubElement(lpn_lock, "InventoryLockCode").text = lock_code

        return ET.tostring(txml, encoding="unicode")

    # ---------- EXPORT RANGE ----------

    def export_range(self, seq):
        file = f"LPN_RANGE_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(file, "w") as f:
            for item in seq:
                f.write(item + "\n")
        self.log(f"LPN range exported: {file}")

    # ---------- THREAD ----------

    def start_thread(self):
        t = threading.Thread(target=self.bulk_publish)
        t.daemon = True
        t.start()

    # ---------- BULK PUBLISH ----------

    def bulk_publish(self):
        topic = self.topic_var.get()
        if not topic:
            messagebox.showerror("Error", "Please set the Kafka Topic first")
            return

        broker, user, pwd, cert = self.get_kafka_config()
        self.log(f"Connecting to broker: {broker}")
        self.log(f"Using SSL Truststore: {cert} (Exists: {os.path.exists(cert)})")
        
        qty = int(self.qty_var.get())
        batch = int(self.batch_var.get())

        lpn_seq = self.generate_sequence(self.start_lpn_var.get(), qty)
        parent_lpn_seq = [self.start_parent_lpn_var.get()] * qty if self.start_parent_lpn_var.get() else [""] * qty

        # Preview the first XML message
        first_xml = self.generate_xml(lpn_seq[0], parent_lpn_seq[0])
        preview = self.preview_xml_window(first_xml)

        if not preview["confirm"]:
            self.log("Publish cancelled by user.")
            return

        first_xml = preview["xml"]

        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: v.encode("utf-8"),
                security_protocol="SASL_SSL",
                sasl_mechanism="SCRAM-SHA-512",
                sasl_plain_username=user,
                sasl_plain_password=pwd,
                ssl_cafile=cert,
                ssl_check_hostname=False,
                api_version=(2, 0, 0)
            )

            sent = 0
            start = time.time()

            for i in range(qty):
                if i == 0:
                    xml = first_xml
                else:
                    # In bulk, we increment the sequence starting values for the next message
                    # The message generation handles row offset LPN increments internally.
                    current_lpn_start = self.generate_sequence(self.start_lpn_var.get(), qty * len(self.lpn_items))[i * len(self.lpn_items)]
                    current_parent_start = self.start_parent_lpn_var.get()
                    xml = self.generate_xml(current_lpn_start, current_parent_start)

                producer.send(topic, value=xml)
                sent += 1

                if sent % batch == 0:
                    producer.flush()

                progress = (sent / qty) * 100
                self.progress_var.set(progress)

            producer.flush()
            producer.close()

            elapsed = round(time.time() - start, 2)
            self.export_range(lpn_seq)
            self.log(f"Completed {qty} messages in {elapsed} sec")
            messagebox.showinfo("Done", f"{qty} LPN conversion messages published")

        except Exception as e:
            self.log(f"Error publishing to Kafka: {str(e)}")
            messagebox.showerror("Kafka Publish Error", str(e))


# ---------- START APP ----------

if __name__ == "__main__":
    root = tk.Tk()
    root.withdraw()
    show_splash(root)
    root.deiconify()
    app = BulkLPNUI(root)
    root.mainloop()