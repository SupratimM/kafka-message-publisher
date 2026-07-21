import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
import threading
import re
import time
from datetime import datetime
import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
import sys


# ---------- RESOURCE PATH (FOR PYINSTALLER) ----------

def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
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
        text="4Wall DO Injector",
        font=("Segoe UI",26,"bold"),
        bg="#1E1E1E",
        fg="#4FC3F7"
    )
    title.pack(pady=10)

    subtitle = tk.Label(
        frame,
        text="Bulk Distribution Order Publisher",
        font=("Segoe UI",12),
        bg="#1E1E1E",
        fg="#BBBBBB"
    )
    subtitle.pack()

    version = tk.Label(
        frame,
        text="Version 1.1",
        font=("Segoe UI",9),
        bg="#1E1E1E",
        fg="#888888"
    )
    version.pack(pady=10)

    splash.update()
    splash.after(2000, splash.destroy)


# ---------- MAIN APP ----------

class BulkDOUI:

    def __init__(self, root):

        self.root = root
        self.root.title("4Wall DO Injector")

        width = 1100
        height = 720

        screen_w = root.winfo_screenwidth()
        screen_h = root.winfo_screenheight()

        x = int((screen_w/2)-(width/2))
        y = int((screen_h/2)-(height/2))

        root.geometry(f"{width}x{height}+{x}+{y}")
        root.configure(bg="#1E1E1E")

        self.apply_styles()

        self.env_var = tk.StringVar()
        self.topic_var = tk.StringVar()
        self.dc_var = tk.StringVar()

        self.start_do_var = tk.StringVar()
        self.qty_var = tk.StringVar()
        self.batch_var = tk.StringVar(value="100")

        self.progress_var = tk.DoubleVar()

        self.txml_template = None
        self.line_items = []

        self.build_ui()

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
            text="4Wall DO Injector",
            font=("Segoe UI",22,"bold"),
            bg="#1E1E1E",
            fg="#4FC3F7"
        )
        title.pack(pady=(10,0))

        subtitle = tk.Label(
            self.root,
            text="Bulk Distribution Order Publisher",
            font=("Segoe UI",11),
            bg="#1E1E1E",
            fg="#BBBBBB"
        )
        subtitle.pack(pady=(0,10))

        main = ttk.Frame(self.root)
        main.pack(fill=tk.BOTH, expand=True, padx=15)

        env_frame = ttk.LabelFrame(main, text="Environment")
        env_frame.pack(fill=tk.X, pady=5)

        ttk.Label(env_frame,text="Environment").grid(row=0,column=0,padx=5,pady=5)

        env_combo = ttk.Combobox(
            env_frame,
            textvariable=self.env_var,
            values=["EQA1","EQA2","EQA3"],
            width=10
        )

        env_combo.grid(row=0,column=1)
        env_combo.bind("<<ComboboxSelected>>", self.update_topic)

        ttk.Label(env_frame,text="Topic").grid(row=0,column=2,padx=10)

        ttk.Entry(
            env_frame,
            textvariable=self.topic_var,
            width=45
        ).grid(row=0,column=3)

        ttk.Button(
            env_frame,
            text="Fetch Topics",
            command=self.fetch_topics
        ).grid(row=0,column=4,padx=5)

        ttk.Button(
            env_frame,
            text="Upload TXML",
            command=self.upload_txml
        ).grid(row=0,column=5,padx=5)

        config = ttk.LabelFrame(main,text="DO Configuration")
        config.pack(fill=tk.X,pady=5)

        ttk.Label(config,text="DC").grid(row=0,column=0,padx=5,pady=5)
        ttk.Entry(config,textvariable=self.dc_var,width=12).grid(row=0,column=1)

        ttk.Label(config,text="Start DO").grid(row=0,column=2)
        ttk.Entry(config,textvariable=self.start_do_var,width=20).grid(row=0,column=3)

        ttk.Label(config,text="Messages").grid(row=0,column=4)
        ttk.Entry(config,textvariable=self.qty_var,width=10).grid(row=0,column=5)

        ttk.Label(config,text="Batch Size").grid(row=0,column=6)
        ttk.Entry(config,textvariable=self.batch_var,width=10).grid(row=0,column=7)

        container = ttk.LabelFrame(main,text="Line Items")
        container.pack(fill=tk.BOTH,pady=10)

        canvas = tk.Canvas(container,height=150,bg="#252526",highlightthickness=0)
        scrollbar = ttk.Scrollbar(container,orient="vertical",command=canvas.yview)

        self.item_frame = ttk.Frame(canvas)

        self.item_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0,0),window=self.item_frame,anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left",fill="both",expand=True)
        scrollbar.pack(side="right",fill="y")

        ttk.Button(
            self.item_frame,
            text="+ Add Line Item",
            command=self.add_line_item
        ).grid(row=0,column=0,pady=5)

        ttk.Label(self.item_frame,text="Item").grid(row=1,column=1)
        ttk.Label(self.item_frame,text="Qty").grid(row=1,column=3)

        self.add_line_item()

        actions = ttk.Frame(main)
        actions.pack(pady=10)

        ttk.Button(
            actions,
            text="Preview Range",
            command=self.preview_range
        ).pack(side=tk.LEFT,padx=10)

        ttk.Button(
            actions,
            text="START BULK PUBLISH",
            command=self.start_thread
        ).pack(side=tk.LEFT,padx=10)

        ttk.Label(main,text="Publishing Progress").pack()

        self.progress = ttk.Progressbar(
            main,
            variable=self.progress_var,
            maximum=100,
            length=500
        )
        self.progress.pack(pady=5)

        log_frame = ttk.LabelFrame(main,text="Console")
        log_frame.pack(fill=tk.BOTH,expand=True,pady=10)

        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            font=("Consolas",10),
            bg="#111111",
            fg="#00FF9C",
            insertbackground="white"
        )
        self.log_text.pack(fill=tk.BOTH,expand=True)

    # ---------- TOPIC AUTO SET ----------

    def update_topic(self, event=None):

        env = self.env_var.get()

        if not env:
            return

        env_num = env[3:]
        topic = f"QA{env_num}.L.WMOS.DISTRIBUTIONORDER.PUBLISH"

        self.topic_var.set(topic)

    # ---------- KAFKA CONFIG ----------

    def get_kafka_config(self):

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
            messagebox.showerror("Error","Select Environment first")
            return

        try:

            broker,user,pwd,cert = self.get_kafka_config()

            admin = KafkaAdminClient(
                bootstrap_servers=broker,
                security_protocol="SASL_SSL",
                sasl_mechanism="SCRAM-SHA-512",
                sasl_plain_username=user,
                sasl_plain_password=pwd,
                ssl_cafile=cert
            )

            topics = admin.list_topics()

            env_num = env[3:]
            env_topics=[t for t in topics if f"QA{env_num}" in t]

            if not env_topics:
                messagebox.showinfo("Topics","No topics found")
                return

            self.show_topic_selector(env_topics)

        except Exception as e:
            messagebox.showerror("Kafka Error",str(e))

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

        ttk.Button(container,text="Use Selected Topic",command=select_topic).pack(pady=10)

        search_entry.focus()

    # ---------- LINE ITEMS ----------

    def add_line_item(self):

        row=len(self.line_items)+2

        item_var=tk.StringVar()
        qty_var=tk.StringVar()

        item=ttk.Entry(self.item_frame,textvariable=item_var,width=20)
        item.grid(row=row,column=1,padx=5)

        qty=ttk.Entry(self.item_frame,textvariable=qty_var,width=10)
        qty.grid(row=row,column=3)

        delete=ttk.Button(
            self.item_frame,
            text="Delete",
            command=lambda r=row:self.delete_line_item(r)
        )
        delete.grid(row=row,column=4)

        self.line_items.append((item,qty,delete,item_var,qty_var))

    def delete_line_item(self,row):

        index=row-2

        if index>=len(self.line_items):
            return

        widgets=self.line_items[index][:3]

        for w in widgets:
            w.destroy()

        self.line_items.pop(index)

        self.refresh_items()

    def refresh_items(self):

        for i,item in enumerate(self.line_items,start=2):

            entry_item,entry_qty,btn,item_var,qty_var=item

            entry_item.grid(row=i,column=1)
            entry_qty.grid(row=i,column=3)

            btn.config(command=lambda r=i:self.delete_line_item(r))
            btn.grid(row=i,column=4)

    # ---------- LOG ----------

    def log(self,msg):

        t=datetime.now().strftime("%H:%M:%S")

        self.log_text.insert(tk.END,f"[{t}] {msg}\n")
        self.log_text.see(tk.END)

        self.root.update()

    # ---------- UPLOAD TXML ----------

    def upload_txml(self):

        file=filedialog.askopenfilename(
            title="Select TXML",
            filetypes=[("XML Files","*.xml")]
        )

        if not file:
            return

        tree=ET.parse(file)
        self.txml_template=tree.getroot()

        self.log("TXML Template Loaded")

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
        preview.title("Preview Generated TXML")
        preview.geometry("900x600")

        text = scrolledtext.ScrolledText(preview,font=("Consolas",10))
        text.pack(fill=tk.BOTH,expand=True)

        text.insert("1.0", self.pretty_xml(xml))

        result={"confirm":False,"xml":xml}

        def confirm():

            edited=text.get("1.0",tk.END).strip()

            result["confirm"]=True
            result["xml"]=edited

            preview.destroy()

        ttk.Button(preview,text="Confirm & Send",command=confirm).pack(pady=10)

        preview.grab_set()
        self.root.wait_window(preview)

        return result

    # ---------- DO SEQUENCE ----------

    def generate_sequence(self,start,count):

        match=re.match(r"(.*?)(\d+)$",start)

        prefix=match.group(1)
        number=int(match.group(2))
        padding=len(match.group(2))

        seq=[]

        for i in range(count):
            seq.append(prefix+str(number+i).zfill(padding))

        return seq

    # ---------- PREVIEW RANGE ----------

    def preview_range(self):

        start=self.start_do_var.get()
        qty=int(self.qty_var.get())

        seq=self.generate_sequence(start,qty)

        messagebox.showinfo(
            "DO Range",
            f"Start : {seq[0]}\nEnd : {seq[-1]}\nTotal : {qty}"
        )

    # ---------- XML GENERATOR ----------

    def generate_xml(self,do):

        root=ET.fromstring(ET.tostring(self.txml_template))

        dc=self.dc_var.get()

        reference_id=dc+do

        ref=root.find(".//Reference_ID")
        if ref is not None:
            ref.text=reference_id

        ext=root.find(".//External_Reference_ID")
        if ext is not None:
            ext.text=reference_id

        do_id=root.find(".//DistributionOrderId")
        if do_id is not None:
            do_id.text=do

        dc_tag=root.find(".//OriginFacilityAliasId")
        if dc_tag is not None:
            dc_tag.text=dc

        line_item_template=root.find(".//LineItem")
        parent=root.find(".//DistributionOrder")

        parent.remove(line_item_template)

        for idx,item in enumerate(self.line_items,start=1):

            item_name=item[3].get()
            qty=item[4].get()

            new_line=ET.fromstring(ET.tostring(line_item_template))

            ln=new_line.find(".//DoLineNbr")
            if ln is not None:
                ln.text=str(idx)

            nm=new_line.find(".//ItemName")
            if nm is not None:
                nm.text=item_name

            qt=new_line.find(".//OrderQty")
            if qt is not None:
                qt.text=qty

            parent.append(new_line)

        return ET.tostring(root,encoding="unicode")

    # ---------- EXPORT RANGE ----------

    def export_range(self,seq):

        file=f"DO_RANGE_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        with open(file,"w") as f:
            for d in seq:
                f.write(d+"\n")

        self.log(f"DO range exported: {file}")

    # ---------- THREAD ----------

    def start_thread(self):

        t=threading.Thread(target=self.bulk_publish)
        t.daemon=True
        t.start()

    # ---------- BULK PUBLISH ----------

    def bulk_publish(self):

        topic=self.topic_var.get()

        broker,user,pwd,cert=self.get_kafka_config()

        qty=int(self.qty_var.get())
        batch=int(self.batch_var.get())

        seq=self.generate_sequence(self.start_do_var.get(),qty)

        first_xml=self.generate_xml(seq[0])

        preview=self.preview_xml_window(first_xml)

        if not preview["confirm"]:
            return

        first_xml=preview["xml"]

        producer=KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v:v.encode("utf-8"),
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=user,
            sasl_plain_password=pwd,
            ssl_cafile=cert,
            ssl_check_hostname=False
        )

        sent=0
        start=time.time()

        for i,do in enumerate(seq):

            if i==0:
                xml=first_xml
            else:
                xml=self.generate_xml(do)

            producer.send(topic,value=xml)

            sent+=1

            if sent%batch==0:
                producer.flush()

            progress=(sent/qty)*100
            self.progress_var.set(progress)

        producer.flush()
        producer.close()

        elapsed=round(time.time()-start,2)

        self.export_range(seq)

        self.log(f"Completed {qty} messages in {elapsed} sec")

        messagebox.showinfo("Done",f"{qty} DO messages published")


# ---------- START APP ----------

if __name__=="__main__":

    root=tk.Tk()
    root.withdraw()

    show_splash(root)

    root.deiconify()

    app=BulkDOUI(root)

    root.mainloop()