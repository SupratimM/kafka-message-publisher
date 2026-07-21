import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
from kafka import KafkaProducer
import threading
import re
import time
from datetime import datetime
import xml.etree.ElementTree as ET


class BulkDOUI:

    def __init__(self, root):

        self.root = root
        self.root.title("Bulk DO Kafka Publisher")
        self.root.geometry("900x650")

        self.env_var = tk.StringVar()
        self.dc_var = tk.StringVar()

        self.start_do_var = tk.StringVar()
        self.qty_var = tk.StringVar()
        self.batch_var = tk.StringVar(value="100")

        self.item1_var = tk.StringVar()
        self.qty1_var = tk.StringVar()

        self.progress_var = tk.DoubleVar()

        self.txml_template = None

        self.build_ui()

    # ---------------- UI ---------------- #

    def build_ui(self):

        frame = ttk.Frame(self.root, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)

        env_frame = ttk.LabelFrame(frame, text="Environment")
        env_frame.pack(fill=tk.X)

        ttk.Label(env_frame, text="Environment").grid(row=0, column=0)

        ttk.Combobox(
            env_frame,
            textvariable=self.env_var,
            values=["EQA1", "EQA2", "EQA3"],
            width=10
        ).grid(row=0, column=1)

        file_frame = ttk.LabelFrame(frame, text="TXML Template")
        file_frame.pack(fill=tk.X, pady=5)

        ttk.Button(
            file_frame,
            text="Upload TXML",
            command=self.upload_txml
        ).pack(side=tk.LEFT, padx=5)

        do_frame = ttk.LabelFrame(frame, text="DO Configuration")
        do_frame.pack(fill=tk.X, pady=5)

        ttk.Label(do_frame, text="DC").grid(row=0, column=0)
        ttk.Entry(do_frame, textvariable=self.dc_var).grid(row=0, column=1)

        ttk.Label(do_frame, text="Starting DO").grid(row=1, column=0)
        ttk.Entry(do_frame, textvariable=self.start_do_var).grid(row=1, column=1)

        ttk.Label(do_frame, text="Messages").grid(row=2, column=0)
        ttk.Entry(do_frame, textvariable=self.qty_var).grid(row=2, column=1)

        ttk.Label(do_frame, text="Batch Size").grid(row=3, column=0)
        ttk.Entry(do_frame, textvariable=self.batch_var).grid(row=3, column=1)

        item_frame = ttk.LabelFrame(frame, text="Line Items")
        item_frame.pack(fill=tk.X, pady=5)

        ttk.Label(item_frame, text="Item1").grid(row=0, column=0)
        ttk.Entry(item_frame, textvariable=self.item1_var).grid(row=0, column=1)

        ttk.Label(item_frame, text="Qty1").grid(row=0, column=2)
        ttk.Entry(item_frame, textvariable=self.qty1_var).grid(row=0, column=3)

        btn_frame = ttk.Frame(frame)
        btn_frame.pack(pady=10)

        ttk.Button(btn_frame, text="Preview Range", command=self.preview_range).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Start Bulk Publish", command=self.start_thread).pack(side=tk.LEFT, padx=5)

        self.progress = ttk.Progressbar(
            frame,
            variable=self.progress_var,
            maximum=100
        )
        self.progress.pack(fill=tk.X)

        log_frame = ttk.LabelFrame(frame, text="Status")
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = scrolledtext.ScrolledText(log_frame)
        self.log_text.pack(fill=tk.BOTH, expand=True)

    # ---------------- Logging ---------------- #

    def log(self, msg):

        t = datetime.now().strftime("%H:%M:%S")

        self.log_text.insert(tk.END, f"[{t}] {msg}\n")
        self.log_text.see(tk.END)

        self.root.update()

    # ---------------- Upload TXML ---------------- #

    def upload_txml(self):

        file_path = filedialog.askopenfilename(
            title="Select TXML File",
            filetypes=[("XML Files", "*.xml"), ("TXML Files", "*.txml")]
        )

        if not file_path:
            return

        tree = ET.parse(file_path)
        self.txml_template = tree.getroot()

        self.log(f"TXML template loaded: {file_path}")

    # ---------------- Topic ---------------- #

    def get_topic(self, env):

        env_num = env[3:]
        return f"QA{env_num}.L.WMOS.DISTRIBUTIONORDER.PUBLISH"

    # ---------------- Kafka config ---------------- #

    def get_kafka_config(self):

        return (
            "rk.qa.kafka.logistics.wsgc.com:443",
            "appwmos",
            "n3w0rk"
        )

    # ---------------- DO sequence ---------------- #

    def generate_sequence(self, start, count):

        match = re.match(r"(.*?)(\d+)$", start)

        prefix = match.group(1)
        number = int(match.group(2))
        padding = len(match.group(2))

        seq = []

        for i in range(count):
            seq.append(prefix + str(number + i).zfill(padding))

        return seq

    # ---------------- Preview Range ---------------- #

    def preview_range(self):

        start = self.start_do_var.get()
        qty = int(self.qty_var.get())

        seq = self.generate_sequence(start, qty)

        messagebox.showinfo(
            "DO Range",
            f"Start : {seq[0]}\nEnd : {seq[-1]}\nTotal : {qty}"
        )

    # ---------------- XML Generator ---------------- #

    def generate_xml(self, do):

        if self.txml_template is None:
            messagebox.showerror("Error", "Upload TXML template first")
            return ""

        root = ET.fromstring(ET.tostring(self.txml_template))

        dc = self.dc_var.get()
        item1 = self.item1_var.get()
        quantity1 = self.qty1_var.get()

        reference_id = dc + do
        external_reference_id = dc + do

        ref = root.find(".//Reference_ID")
        if ref is not None:
            ref.text = reference_id

        ext_ref = root.find(".//External_Reference_ID")
        if ext_ref is not None:
            ext_ref.text = external_reference_id

        do_id = root.find(".//DistributionOrderId")
        if do_id is not None:
            do_id.text = do

        dc_tag = root.find(".//OriginFacilityAliasId")
        if dc_tag is not None:
            dc_tag.text = dc

        item_tag = root.find(".//ItemName")
        if item_tag is not None:
            item_tag.text = item1

        qty_tag = root.find(".//OrderQty")
        if qty_tag is not None:
            qty_tag.text = quantity1

        return ET.tostring(root, encoding="unicode")

    # ---------------- Preview XML Window ---------------- #

    def preview_xml_window(self, xml):

        preview = tk.Toplevel(self.root)
        preview.title("Preview First Generated TXML")
        preview.geometry("800x600")

        text = scrolledtext.ScrolledText(
            preview,
            wrap=tk.NONE,
            font=("Courier New", 10)
        )
        text.pack(fill=tk.BOTH, expand=True)

        text.insert(tk.END, xml)
        text.config(state="disabled")

        result = {"confirm": False}

        def confirm():
            result["confirm"] = True
            preview.destroy()

        def cancel():
            preview.destroy()

        button_frame = ttk.Frame(preview)
        button_frame.pack(pady=10)

        ttk.Button(button_frame, text="Confirm & Send", command=confirm).pack(side=tk.LEFT, padx=10)
        ttk.Button(button_frame, text="Cancel", command=cancel).pack(side=tk.LEFT, padx=10)

        preview.grab_set()
        self.root.wait_window(preview)

        return result["confirm"]

    # ---------------- Export DO Range ---------------- #

    def export_range(self, seq):

        filename = f"DO_RANGE_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        with open(filename, "w") as f:
            for d in seq:
                f.write(d + "\n")

        self.log(f"DO range exported: {filename}")

    # ---------------- Start Thread ---------------- #

    def start_thread(self):

        t = threading.Thread(target=self.bulk_publish)
        t.daemon = True
        t.start()

    # ---------------- Bulk Publish ---------------- #

    def bulk_publish(self):

        env = self.env_var.get()

        topic = self.get_topic(env)

        broker, user, pwd = self.get_kafka_config()

        qty = int(self.qty_var.get())
        batch = int(self.batch_var.get())

        seq = self.generate_sequence(self.start_do_var.get(), qty)

        first_xml = self.generate_xml(seq[0])

        confirm = self.preview_xml_window(first_xml)

        if not confirm:
            return

        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: v.encode("utf-8"),
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=user,
            sasl_plain_password=pwd,
            ssl_check_hostname=False
        )

        sent = 0
        start = time.time()

        for do in seq:

            xml = self.generate_xml(do)

            producer.send(topic, value=xml)

            sent += 1

            if sent % batch == 0:
                producer.flush()

            progress = (sent / qty) * 100
            self.progress_var.set(progress)

        producer.flush()
        producer.close()

        elapsed = round(time.time() - start, 2)

        self.export_range(seq)

        self.log(f"Completed {qty} messages in {elapsed} sec")

        messagebox.showinfo("Done", f"{qty} DO messages published")


# ---------------- Run Program ---------------- #

if __name__ == "__main__":

    root = tk.Tk()

    app = BulkDOUI(root)

    root.mainloop()