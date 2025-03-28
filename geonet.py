import pandas as pd
import ftplib,gzip,os
from datetime import datetime,timedelta
from contextlib import closing

#############################Setting################################################
#E_NAME setting
NAME          = 'TOKYOCHIYODA'

#Setting OBS & F5 & NAV
OBS           = True
F5            = True
NAV           = True

##GSI FTP setting
Username      = 'ozeki'
Password      = 'A!Y2CaK6SeKtSqS'
Host          = 'terras.gsi.go.jp'

#Date
startdate     = datetime(2025, 3, 1)
enddate       = datetime(2025, 3, 1)

#Outputpath setting (Optional)
Outputpath    = 'rinex'

#############################Setting################################################




def decompress_gz_file(gz_file_path, output_file_path):
    with gzip.open(gz_file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            f_out.write(f_in.read())
            
def delete_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)

def ftp_open(host, username="", password="", use_tls=False):
    try:
        if use_tls:
            ftp = ftplib.FTP_TLS(host)
            ftp.login(username, password)
            ftp.prot_p()
        else:
            ftp = ftplib.FTP(host)
            ftp.login(username, password)
        
        print(f"{host} connected")
        return ftp
    except ftplib.all_errors as e:
        print(f"FTP error: {e}")
        return None

def ftp_close(ftp):
    if ftp:
        ftp.quit()

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


if __name__ == "__main__":
    filepath = "cluster_list_F5.txt"
    df1 = pd.read_csv(filepath, sep=r"\s+", encoding="utf-8")
    
    if Outputpath==None:
        Outputpath=os.getcwd()

    with closing(ftp_open(Host, Username, Password)) as ftp:
        if ftp:
            if OBS:
                current_date = startdate
                while current_date <= enddate:
                    print(current_date.strftime("%Y-%m-%d"))
                    doy = str(current_date.timetuple().tm_yday).zfill(3)
                    path = f"/data/GRJE_3.02/{current_date.year}/{doy}"

                    id = get_id_from_df(df1, NAME)
                    if id:
                        name = id[-4:] + doy + "0." + str(current_date.year % 100) + "o.gz"
                        ftp_download(ftp, path, name,Outputpath)

                    current_date += timedelta(days=1)

            if F5:
                current_date = startdate
                while current_date <= enddate:

                    path      = f"/data/coordinates_F5/GPS/{current_date.year}/"
                    id        = get_id_from_df(df1, NAME)

                    if id:
                        name  = f"{id}.{current_date.year % 100}.pos"
                        ftp_download(ftp, path, name,Outputpath)

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


