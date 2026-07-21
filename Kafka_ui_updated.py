import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from datetime import datetime
import json
import xml.etree.ElementTree as ET
from collections import deque
import os
import traceback
import socket
import re
import time
import sys

def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# Fix socket SSL issues
original_getaddrinfo = socket.getaddrinfo
def patched_getaddrinfo(host, *args, **kwargs):
    return original_getaddrinfo(host, *args, **kwargs)
socket.getaddrinfo = patched_getaddrinfo


class KafkaBridgeUI:

    def __init__(self, root):

        self.root = root
        self.root.title("Kafka Message Publisher")

        self.message_history = deque(maxlen=50)

        self.env_var = tk.StringVar()
        self.kafka_type_var = tk.StringVar()
        self.interface_var = tk.StringVar()

        self.auto_topic_var = tk.StringVar(value="Auto Topic: None")
        self.custom_topic_var = tk.StringVar()
        self.final_topic_var = tk.StringVar(value="Final Topic: None")

        self.format_var = tk.StringVar(value="XML")

        self.tracker_key_var = tk.StringVar()
        self.tracker_result_var = tk.StringVar(value="Status: Not checked")
        
        self.load_history()

        self.setup_ui()

    # ======================================================
    # UI
    # ======================================================

    def load_history(self):

        if os.path.exists("history.json"):

            with open("history.json", "r") as f:
                data = json.load(f)

                self.message_history = deque(data, maxlen=50)
                
    def save_history(self):

        with open("history.json", "w") as f:
            json.dump(list(self.message_history), f, indent=2)

    def check_message_status(self):

        search_key = self.tracker_key_var.get().strip()

        topic = self.final_topic_var.get().replace("Final Topic: ", "")

        if not search_key or not topic:
            messagebox.showerror(
                "Error",
                "Please enter a search key and select a topic first."
            )
            return

        try:

            broker, username, password = self.get_kafka_config(
                self.env_var.get(),
                self.kafka_type_var.get()
            )
            kafka_type = self.kafka_type_var.get()

            config = {
                "bootstrap_servers": broker,
                "auto_offset_reset": "earliest",
                "consumer_timeout_ms": 3000,
                "group_id": "status-checker",
                "enable_auto_commit": False
            }

            if kafka_type == "logistics":
                import ssl
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                config.update({
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "SCRAM-SHA-512",
                    "sasl_plain_username": username,
                    "sasl_plain_password": password,
                    "ssl_context": context
                })
            elif username and password:
                config.update({
                    "security_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": "SCRAM-SHA-512",
                    "sasl_plain_username": username,
                    "sasl_plain_password": password
                })

            consumer = KafkaConsumer(topic, **config)

            found = False

            for msg in consumer:

                value = msg.value.decode("utf-8", errors="ignore")

                if search_key in value:
                    found = True
                    break

            consumer.close()

            if found:
                self.tracker_result_var.set("❌ Message still in topic")
            else:
                self.tracker_result_var.set("✅ Message consumed")

        except Exception as e:

            self.tracker_result_var.set("Error")

            self.log_status(f"Status check failed: {str(e)}")

    def setup_ui(self):

        self.root.geometry("1200x750")

        # HEADER
        header = ttk.Frame(self.root)
        header.pack(fill=tk.X, pady=10)

        tk.Label(header,
                 text="Kafka Message Publisher",
                 font=("Segoe UI",18,"bold")).pack()

        tk.Label(header,
                 text="QA Utility for Publishing Messages to Kafka",
                 font=("Segoe UI",9)).pack()

        main = ttk.Frame(self.root)
        main.pack(fill=tk.BOTH, expand=True, padx=10)

        left = ttk.Frame(main)
        left.pack(side=tk.LEFT, fill=tk.Y)

        right = ttk.Frame(main)
        right.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # ===============================
        # ENVIRONMENT
        # ===============================

        env_frame = ttk.LabelFrame(left,text="Environment & Connection",padding=10)
        env_frame.pack(fill=tk.X,pady=5)

        ttk.Label(env_frame,text="Environment").grid(row=0,column=0)
        ttk.Combobox(env_frame,textvariable=self.env_var,
                     values=["EQA1","EQA2","EQA3"],width=10).grid(row=0,column=1)

        ttk.Label(env_frame,text="Kafka Type").grid(row=1,column=0)
        ttk.Combobox(env_frame,textvariable=self.kafka_type_var,
                     values=["logistics","str","int"],width=10).grid(row=1,column=1)

        ttk.Label(env_frame,text="Interface").grid(row=2,column=0)
        ttk.Combobox(env_frame,textvariable=self.interface_var,
                     values=["PO","DO","ASN","Item","JOB_STATUS","TOTE_COMPLETE","Custom"],
                     width=15).grid(row=2,column=1)

        ttk.Button(env_frame,text="Test Connection",
                   command=self.test_connection).grid(row=3,column=0,columnspan=2,pady=5)

        # ===============================
        # TOPIC CONFIG
        # ===============================

        topic_frame = ttk.LabelFrame(left,text="Topic Configuration",padding=10)
        topic_frame.pack(fill=tk.X,pady=5)

        ttk.Label(topic_frame,textvariable=self.auto_topic_var).pack(anchor="w")

        row = ttk.Frame(topic_frame)
        row.pack(anchor="w")

        ttk.Entry(row,textvariable=self.custom_topic_var,width=25).pack(side=tk.LEFT)

        ttk.Button(row,text="Fetch Topics",
                   command=self.fetch_topics).pack(side=tk.LEFT,padx=5)

        ttk.Label(topic_frame,
                  textvariable=self.final_topic_var,
                  font=("Consolas",10,"bold")).pack(anchor="w",pady=5)

        # ===============================
        # MESSAGE TRACKER
        # ===============================

        tracker = ttk.LabelFrame(left,text="Message Tracker",padding=10)
        tracker.pack(fill=tk.X,pady=5)

        ttk.Label(tracker,text="Search Key").pack(anchor="w")

        ttk.Entry(tracker,textvariable=self.tracker_key_var,width=25).pack(anchor="w")

        ttk.Button(tracker,text="Check Status",
                   command=self.check_message_status).pack(pady=5)

        ttk.Label(tracker,textvariable=self.tracker_result_var).pack(anchor="w")

        # ===============================
        # ACTIONS
        # ===============================

        actions = ttk.Frame(left)
        actions.pack(fill=tk.X,pady=10)

        tk.Button(actions,
                  text="SEND MESSAGE",
                  bg="#2563eb",
                  fg="white",
                  font=("Segoe UI",10,"bold"),
                  command=self.send_to_kafka).pack(fill=tk.X,pady=3)

        ttk.Button(actions,text="Clear",
                   command=self.clear_fields).pack(fill=tk.X)

        # ===============================
        # MESSAGE HISTORY
        # ===============================

        history = ttk.LabelFrame(left,text="Message History",padding=10)
        history.pack(fill=tk.BOTH,expand=True)

        self.history_list = tk.Listbox(history,height=8)
        self.history_list.pack(fill=tk.BOTH,expand=True)

        # ===============================
        # MESSAGE EDITOR
        # ===============================

        msg_frame = ttk.LabelFrame(right,text="Message Editor",padding=10)
        msg_frame.pack(fill=tk.BOTH,expand=True,pady=5)

        toolbar = ttk.Frame(msg_frame)
        toolbar.pack(fill=tk.X)

        ttk.Label(toolbar,text="Format").pack(side=tk.LEFT)

        ttk.Radiobutton(toolbar,text="XML",variable=self.format_var,value="XML").pack(side=tk.LEFT)
        ttk.Radiobutton(toolbar,text="JSON",variable=self.format_var,value="JSON").pack(side=tk.LEFT)
        ttk.Radiobutton(toolbar,text="Text",variable=self.format_var,value="Text").pack(side=tk.LEFT)

        ttk.Button(toolbar,text="Upload File",
                   command=self.upload_file).pack(side=tk.LEFT,padx=10)

        ttk.Button(toolbar,text="Validate",
                   command=self.validate_message).pack(side=tk.LEFT)

        self.file_label = ttk.Label(msg_frame,text="No file selected")
        self.file_label.pack(anchor="w")

        self.message_text = scrolledtext.ScrolledText(
            msg_frame,
            wrap=tk.WORD,
            height=20,
            font=("Consolas",10),
            bg="#0f172a",
            fg="#e2e8f0",
            insertbackground="white"
        )
        self.message_text.pack(fill=tk.BOTH,expand=True)

        self.message_text.bind("<KeyRelease>", self.highlight_syntax)

        # ===============================
        # LOGS
        # ===============================

        log_frame = ttk.LabelFrame(right,text="Kafka Logs",padding=10)
        log_frame.pack(fill=tk.BOTH,pady=5)

        self.status_text = scrolledtext.ScrolledText(
            log_frame,
            height=8,
            font=("Consolas",9),
            bg="#111827",
            fg="#22c55e",
            state="disabled"
        )
        self.status_text.pack(fill=tk.BOTH,expand=True)

        footer = ttk.Label(self.root,
                           text="Kafka Message Publisher | Version 1.1",
                           font=("Segoe UI",8))
        footer.pack(pady=5)

        self.env_var.trace_add("write",self.update_topic_display)
        self.kafka_type_var.trace_add("write",self.update_topic_display)
        self.interface_var.trace_add("write",self.update_topic_display)
        self.custom_topic_var.trace_add("write",self.update_final_topic)
        self.history_list.bind("<<ListboxSelect>>", self.load_history_message)
        
        for entry in self.message_history:
            self.history_list.insert(
                tk.END,
                f"{entry['time']} -> {entry['topic']}"
            )

    # ======================================================
    # TOPIC FUNCTIONS
    # ======================================================

    def load_history_message(self, event):

        selection = self.history_list.curselection()

        if not selection:
            return

        index = selection[0]

        entry = self.message_history[index]

        self.message_text.delete("1.0", tk.END)
        self.message_text.insert("1.0", entry["message"])

        self.custom_topic_var.set(entry["topic"])

    def fetch_topics(self):

        env = self.env_var.get()
        kafka_type = self.kafka_type_var.get()

        if not env or not kafka_type:
            messagebox.showerror("Error","Select Environment and Kafka Type first")
            return

        broker, username, password = self.get_kafka_config(env,kafka_type)

        try:

            config = {"bootstrap_servers":broker}
            if kafka_type == "logistics":
                import ssl
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                config.update({
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "SCRAM-SHA-512",
                    "sasl_plain_username": username,
                    "sasl_plain_password": password,
                    "ssl_context": context
                })

            consumer = KafkaConsumer(**config)

            topics = list(consumer.topics())

            consumer.close()

            self.show_topic_selector(topics)

        except Exception as e:
            messagebox.showerror("Kafka Error",str(e))


    def show_topic_selector(self,topics):

        win = tk.Toplevel(self.root)
        win.title("Select Kafka Topic")
        win.geometry("500x400")

        search_var = tk.StringVar()

        ttk.Entry(win,textvariable=search_var).pack(fill=tk.X,padx=10,pady=5)

        listbox = tk.Listbox(win)
        listbox.pack(fill=tk.BOTH,expand=True,padx=10,pady=5)

        topics = sorted(topics)

        for t in topics:
            listbox.insert(tk.END,t)

        def filter_topics(*args):

            search = search_var.get().lower()

            listbox.delete(0,tk.END)

            for t in topics:
                if search in t.lower():
                    listbox.insert(tk.END,t)

        search_var.trace_add("write",filter_topics)

        def select_topic():

            if not listbox.curselection():
                return

            topic = listbox.get(listbox.curselection())

            self.custom_topic_var.set(topic)

            win.destroy()

        ttk.Button(win,text="Use Selected Topic",
                   command=select_topic).pack(pady=5)

    # ======================================================
    # MESSAGE SEND
    # ======================================================

    def send_to_kafka(self):

        env = self.env_var.get()
        kafka_type = self.kafka_type_var.get()

        broker, username, password = self.get_kafka_config(env, kafka_type)

        topic = self.final_topic_var.get().replace("Final Topic: ", "")

        message = self.message_text.get("1.0", tk.END).strip()

        if not all([env, kafka_type, topic, message]):
            messagebox.showerror("Error", "Fill all fields")
            return

        start = time.time()

        try:

            producer_config = {
                "bootstrap_servers": broker,
                "value_serializer": lambda v: v.encode("utf-8")
            }

            # Logistics Kafka (SSL + SCRAM)
            if kafka_type == "logistics":
                import ssl
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                producer_config.update({
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "SCRAM-SHA-512",
                    "sasl_plain_username": username,
                    "sasl_plain_password": password,
                    "ssl_context": context
                })

            # STR / INT Kafka
            elif username and password:

                producer_config.update({
                    "security_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": "SCRAM-SHA-512",
                    "sasl_plain_username": username,
                    "sasl_plain_password": password
                })

            producer = KafkaProducer(**producer_config)

            producer.send(topic, value=message)

            producer.flush()

            latency = (time.time() - start) * 1000

            producer.close()

            self.log_status(f"Message sent successfully ({latency:.2f} ms)")
            
            messagebox.showinfo(
                "Success",
                f"Message posted to Kafka successfully!\n\nTopic: {topic}\nLatency: {latency:.2f} ms"
            )

            timestamp = datetime.now().strftime("%H:%M:%S")

            history_entry = {
                "time": timestamp,
                "topic": topic,
                "message": message
            }

            self.message_history.appendleft(history_entry)
            
            self.save_history()

            self.history_list.delete(0, tk.END)

            for entry in self.message_history:
                self.history_list.insert(
                    tk.END,
                    f"{entry['time']} -> {entry['topic']}"
                )

        except Exception as e:

            self.log_status(f"Kafka error: {str(e)}")

            messagebox.showerror("Kafka Error", str(e))
    # ======================================================
    # SYNTAX HIGHLIGHT
    # ======================================================

    def highlight_syntax(self,event=None):

        if self.format_var.get() != "XML":
            return

        content = self.message_text.get("1.0",tk.END)

        self.message_text.tag_remove("xml","1.0",tk.END)

        for match in re.finditer(r"</?[\w]+>",content):

            start = f"1.0+{match.start()}c"
            end = f"1.0+{match.end()}c"

            self.message_text.tag_add("xml",start,end)

        self.message_text.tag_config("xml",foreground="#38bdf8")

    # ======================================================
    # OTHER FUNCTIONS (UNCHANGED)
    # ======================================================

    def upload_file(self):
        filepath = filedialog.askopenfilename()
        if filepath:
            with open(filepath,'r') as f:
                content = f.read()
            self.message_text.delete("1.0",tk.END)
            self.message_text.insert("1.0",content)
            self.file_label.config(text=os.path.basename(filepath))

    def validate_message(self):

        content = self.message_text.get("1.0",tk.END).strip()

        try:

            if self.format_var.get()=="XML":
                ET.fromstring(content)

            elif self.format_var.get()=="JSON":
                json.loads(content)

            messagebox.showinfo("Validation","Message is valid")

        except Exception as e:
            messagebox.showerror("Validation Error",str(e))


    def clear_fields(self):
        self.message_text.delete("1.0",tk.END)
        self.custom_topic_var.set("")
        self.file_label.config(text="No file selected")
        self.tracker_key_var.set("")
        self.tracker_result_var.set("Status: Not checked")


    def log_status(self,message):

        ts = datetime.now().strftime("%H:%M:%S")

        self.status_text.configure(state="normal")

        self.status_text.insert(tk.END,f"[{ts}] {message}\n")

        self.status_text.configure(state="disabled")

        self.status_text.see(tk.END)


    def update_topic_display(self,*args):

        env = self.env_var.get()
        kafka_type = self.kafka_type_var.get()
        interface = self.interface_var.get()

        if env and kafka_type and interface:

            topic = self.get_auto_topic_name(env,kafka_type,interface)

            self.auto_topic_var.set(f"Auto Topic: {topic}")

            self.update_final_topic()


    def update_final_topic(self,*args):

        custom = self.custom_topic_var.get().strip()

        if custom:
            self.final_topic_var.set(f"Final Topic: {custom}")

        else:
            env = self.env_var.get()
            kafka_type = self.kafka_type_var.get()
            interface = self.interface_var.get()

            if interface:
                topic = self.get_auto_topic_name(env,kafka_type,interface)
                self.final_topic_var.set(f"Final Topic: {topic}")


    def get_auto_topic_name(self,env,kafka_type,interface):

        env_prefix = "QA"+env[3:] if kafka_type=="str" else env

        topics = {
            "PO":f"{env_prefix}.B.WMOS.PURCHASE.ORDER.CREATE",
            "DO":f"{env_prefix}.L.OMS.SALESORDERDROP.PUBLISH",
            "ASN":f"{env_prefix}.B.SPS.SHIPMENT.CREATE",
            "Item":f"{env_prefix}.B.RMS.ITEM.PUBLISH",
            "JOB_STATUS":f"{env_prefix}.L.PULSE.LMS.PUBLISH",
            "TOTE_COMPLETE":f"{env_prefix}.L.PULSE.MS.TOTE.COMPLETE"
        }

        return topics.get(interface,f"{env_prefix}.{interface}")

    def test_connection(self):

        env = self.env_var.get()
        kafka_type = self.kafka_type_var.get()

        if not env or not kafka_type:
            messagebox.showerror("Error", "Please select Environment and Kafka Type")
            return

        try:

            broker, username, password = self.get_kafka_config(env, kafka_type)

            config = {
                "bootstrap_servers": broker,
                "request_timeout_ms": 5000
            }

            # logistics kafka requires SSL
            if kafka_type == "logistics":
                import ssl
                context = ssl.create_default_context()
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
                config.update({
                    "security_protocol": "SASL_SSL",
                    "sasl_mechanism": "SCRAM-SHA-512",
                    "sasl_plain_username": username,
                    "sasl_plain_password": password,
                    "ssl_context": context
                })

            # str / int kafka
            elif username and password:

                config.update({
                    "security_protocol": "SASL_PLAINTEXT",
                    "sasl_mechanism": "SCRAM-SHA-512",
                    "sasl_plain_username": username,
                    "sasl_plain_password": password
                })

            consumer = KafkaConsumer(**config)

            consumer.close()

            self.log_status("Kafka connection successful")

            messagebox.showinfo("Success", "Kafka connection successful!")

        except Exception as e:

            self.log_status(f"Connection failed: {str(e)}")

            messagebox.showerror("Connection Failed", str(e))


    def get_kafka_config(self,env,kafka_type):

        brokers = {
            "EQA1":{
                "logistics":("rk.qa.kafka.logistics.wsgc.com:443","appwmos","n3w0rk"),
                "str":("rk.qa.kafka.str.wsgc.com:80","",""),
                "int":("rk.qa.kafka.int.wsgc.com:80","","")
            },
            "EQA2":{
                "logistics":("rk.qa.kafka.logistics.wsgc.com:443","appwmos","n3w0rk"),
                "str":("rk.qa.kafka.str.wsgc.com:80","",""),
                "int":("rk.qa.kafka.int.wsgc.com:80","","")
            },
            "EQA3":{
                "logistics":("rk.qa.kafka.logistics.wsgc.com:443","appwmos","n3w0rk"),
                "str":("rk.qa.kafka.str.wsgc.com:80","",""),
                "int":("rk.qa.kafka.int.wsgc.com:80","","")
            }
        }

        return brokers.get(env,{}).get(kafka_type,("","",""))


if __name__ == "__main__":

    root = tk.Tk()

    app = KafkaBridgeUI(root)

    root.mainloop()