import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import ftplib, gzip, os
from datetime import datetime, timedelta
from contextlib import closing


def decompress_gz_file(gz_file_path, output_file_path):
    with gzip.open(gz_file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            f_out.write(f_in.read())

def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

def ftp_open(host, username="", password="", use_tls=False):
    try:
        ftp = ftplib.FTP_TLS(host) if use_tls else ftplib.FTP(host)
        ftp.login(username, password)
        if use_tls:
            ftp.prot_p()
        print(f"{host} connected")
        return ftp
    except ftplib.all_errors as e:
        print(f"FTP error: {e}")
        return None

def ftp_download(ftp, fpath, name, output_folder):
    try:
        os.makedirs(output_folder, exist_ok=True)
        original_path = ftp.pwd()
        ftp.cwd(fpath)
        files_list = ftp.nlst()

        for file in files_list:
            if name in file:
                local_file_path = os.path.join(output_folder, file)
                print(f"Downloading: {file} -> {local_file_path}")
                with open(local_file_path, "wb") as local_file:
                    ftp.retrbinary("RETR " + file, local_file.write)
                output_file_path = os.path.splitext(local_file_path)[0]
                if file.endswith(".gz"):
                    decompress_gz_file(local_file_path, output_file_path)
                    delete_file(local_file_path)
        ftp.cwd(original_path)
    except ftplib.all_errors as e:
        print(f"FTP error: {e}")

def get_id_from_df(df, name):
    row = df[df["E_NAME"] == name]
    if not row.empty:
        return str(row["ID"].values[0])
    else:
        print(f"Warning: {name} not found in dataframe.")
        return None

# GUI Application using Tkinter

root = tk.Tk()
root.title("GEONET-data-downloader")

# Functions for GUI
def browse_folder():
    folder_selected = filedialog.askdirectory()
    output_entry.delete(0, tk.END)
    output_entry.insert(0, folder_selected)

def run_download():
    NAME = name_entry.get()
    OBS, F5, NAV = obs_var.get(), f5_var.get(), nav_var.get()
    Outputpath = output_entry.get() or 'rinex'
    startdate = datetime.strptime(start_entry.get(), "%Y-%m-%d")
    enddate = datetime.strptime(end_entry.get(), "%Y-%m-%d")
    username = username_entry.get()
    password = password_entry.get()

    df1 = pd.read_csv("cluster_list_F5.txt", sep=r"\s+", encoding="utf-8")

    ftp = ftp_open('terras.gsi.go.jp', username, password)

    if ftp is not None:
        with closing(ftp):
            current_date = startdate
            if OBS:
                while current_date <= enddate:
                    doy = str(current_date.timetuple().tm_yday).zfill(3)
                    path = f"/data/GRJE_3.02/{current_date.year}/{doy}"
                    id = get_id_from_df(df1, NAME)
                    if id:
                        name = id[-4:] + doy + "0." + str(current_date.year % 100) + "o.gz"
                        ftp_download(ftp, path, name, Outputpath)
                    current_date += timedelta(days=1)
            if F5:
                current_date = startdate
                while current_date <= enddate:
                    path = f"/data/coordinates_F5/GPS/{current_date.year}/"
                    id = get_id_from_df(df1, NAME)
                    if id:
                        name = f"{id}.{current_date.year % 100}.pos"
                        ftp_download(ftp, path, name, Outputpath)
                    current_date = current_date.replace(year=current_date.year + 1)
            if NAV:
                with closing(ftp_open("gdc.cddis.eosdis.nasa.gov", "anonymous", "email", use_tls=True)) as ftp:
                    if ftp:
                        current_date = startdate
                        while current_date <= enddate:
                            YYYY      = str(current_date.year)
                            DDD       = str(current_date.timetuple().tm_yday).zfill(3)
                            HH        = "00"
                            DD        = "00"

                            filename  = f"BRDC00IGS_R_{YYYY}{DDD}{HH}{DD}_01D_MN.rnx.gz"
                            directory = f"gps/data/daily/{YYYY}/brdc/"

                            name      = filename.split(".")[0] + "." + filename.split(".")[1]
                            ftp_download(ftp, directory, name, Outputpath)

                            current_date += timedelta(days=1)

    else:
        messagebox.showerror("エラー", "GSI FTPサーバーへのログインに失敗しました。")

    messagebox.showinfo("完了", "処理が完了しました")

# GUI Layout
tk.Label(root, text="ステーション名(E_NAME)").grid(row=0, column=0)
name_entry = tk.Entry(root)
name_entry.insert(0, "TOKYOCHIYODA")
name_entry.grid(row=0, column=1)

tk.Label(root, text="開始日 (YYYY-MM-DD)").grid(row=1, column=0)
start_entry = tk.Entry(root)
start_entry.insert(0, "2025-03-01")
start_entry.grid(row=1, column=1)

tk.Label(root, text="終了日 (YYYY-MM-DD)").grid(row=2, column=0)
end_entry = tk.Entry(root)
end_entry.insert(0, "2025-03-01")
end_entry.grid(row=2, column=1)

tk.Label(root, text="FTPユーザー名").grid(row=3, column=0)
username_entry = tk.Entry(root)
username_entry.grid(row=3, column=1)

tk.Label(root, text="FTPパスワード").grid(row=4, column=0)
password_entry = tk.Entry(root, show="*")
password_entry.grid(row=4, column=1)

tk.Label(root, text="出力先フォルダ").grid(row=5, column=0)
output_entry = tk.Entry(root)
#output_entry.insert(0, "rinex")
output_entry.grid(row=5, column=1)
tk.Button(root, text="参照", command=browse_folder).grid(row=5, column=2)

obs_var = tk.BooleanVar(value=True)
f5_var = tk.BooleanVar(value=True)
nav_var = tk.BooleanVar(value=True)
tk.Checkbutton(root, text="OBS", variable=obs_var).grid(row=6, column=0)
tk.Checkbutton(root, text="F5", variable=f5_var).grid(row=6, column=1)
tk.Checkbutton(root, text="NAV", variable=nav_var).grid(row=6, column=2)

tk.Button(root, text="実行", command=run_download).grid(row=7, column=0, columnspan=3)

root.mainloop()