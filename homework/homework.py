"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
from pathlib import Path
import zipfile
import pandas as pd
def append_df_to_csv(df: pd.DataFrame, path: Path):
    """Escribe df a CSV, creando header si el archivo no existe (append otherwise)."""
    df.to_csv(path, mode='a', header=(not path.exists()), index=False)

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    INPUT_DIR = Path("files/input")
    OUTPUT_DIR = Path("files/output")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # nombres de archivos de salida (sin comprimir)
    CLIENT_OUT = OUTPUT_DIR/"client.csv"
    CAMPAIGN_OUT = OUTPUT_DIR/"campaign.csv"
    ECONOMICS_OUT = OUTPUT_DIR/"economics.csv"

    import os

# Limpiar salidas previas
    for path in [CLIENT_OUT, CAMPAIGN_OUT, ECONOMICS_OUT]:
        if os.path.exists(path):
            os.remove(path)

    for zip_path in INPUT_DIR.glob("*.zip"):
        print(f"\nZIP encontrado: {zip_path.name}")
        with zipfile.ZipFile(zip_path, 'r') as z:
            # lista de archivos dentro del zip
            for member in z.namelist():
                if not member.lower().endswith('.csv'):
                    continue
                print("  leyendo archivo dentro del zip:", member)
                with z.open(member) as f:
                    # lee por chunks para archivos grandes (ajusta chunksize según memoria)
                    reader = pd.read_csv(f, chunksize=100_000, dtype=str, low_memory=False)
                    for chunk in reader:
                        # normaliza nombres de columna (por si tienen espacios)
                        chunk.columns = [c.strip() for c in chunk.columns]

                        # --- Ejemplo: construir client_df con columnas esperadas ---
                        # Ajusta los nombres de columna según tu csv real
                        client_cols = ['client_id','age','job','marital','education','credit_default','mortgage']
                        if all(col in chunk.columns for col in client_cols):
                            client = chunk[client_cols].copy()
                            # transforms de ejemplo (ver más abajo para explicación)
                            client['job'] = client['job'].astype(str).str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
                            client['education'] = (client['education']
                                                .astype('string')
                                                .replace('unknown', pd.NA)
                                                .str.replace('.', '_', regex=False))
                            for col in ['credit_default','mortgage']:
                                client[col] = client[col].astype(str).str.strip().str.lower().eq('yes').astype(int)
                            
                            append_df_to_csv(client, CLIENT_OUT)

                        # --- Ejemplo: construir campaign_df ---
                        # mapping típico para dataset tipo "bank marketing" (ajusta si tu CSV usa otros nombres)
                        campaign_cols = ['client_id','number_contacts','contact_duration','previous_campaign_contacts','previous_outcome','campaign_outcome','day','month']

                        if all(col in chunk.columns for col in campaign_cols):
                            camp = chunk[campaign_cols].copy()
   

 

                            # Transformaciones pero manteniendo nombres originales:
                            camp['previous_outcome'] = camp['previous_outcome'].astype(str).str.lower().eq('success').astype(int)
                            camp['campaign_outcome'] = camp['campaign_outcome'].astype(str).str.lower().eq('yes').astype(int)

                            # Crear columna nueva last_contact_day
                            camp['last_contact_date'] = pd.to_datetime(
                                camp['day'].astype(str) + ' ' + camp['month'].astype(str) + ' 2022',
                                errors='coerce'
                            ).dt.strftime('%Y-%m-%d')

                            # Elimina las columnas day y month si ya no las quieres
                            camp = camp.drop(columns=['day', 'month'])
                            

                            append_df_to_csv(camp, CAMPAIGN_OUT)

                       
                            
                        economy_cols = ['client_id','cons_price_idx','euribor_three_months']
                        if all(col in chunk.columns for col in economy_cols):
                            eco = chunk[economy_cols].copy()

                            
                            append_df_to_csv(eco, ECONOMICS_OUT)
    return


if __name__ == "__main__":
    clean_campaign_data()
    campaignpath="files/output/campaign.csv"
    csv=pd.read_csv(campaignpath)
    print(csv.shape)
