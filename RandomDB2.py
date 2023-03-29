
import sqlite3

import numpy as np
import random
import string
import json
import time
import os, sys
#import shutil
import pathlib
from datetime import date
from dask.distributed import Client
import numpy as np
import random
import string
import dask.dataframe as dd
from dask.distributed import LocalCluster, Client



cluster = LocalCluster(n_workers=2,
                       threads_per_worker=2,
                       memory_target_fraction=0.55,
                       memory_limit='8GB')
client = Client(cluster)
client

def getDataFiles_general_Driver(nombre_Tipo_reporte,nombre_archivo,collection):

    

    df = dd.read_csv(r'E:\temp\mongodb\db\PLD_Clientes_Usuarios_306.csv') 
    #df = pd.read_sql_query('select * from "{}"'.format(collection),con=connPostgre)
    
    #df = pdm.read_mongo("PLD_Creditos", query, "mongodb://172.16.233.219:27017/suptechanalytics")
    #print(df.values[0:2]) # Only values where A > 1 is returned
    #print('\nAAAAAAAA\n')
    #print(df.head())
    return df

#%%
def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def randomize_string(s, chars=string.ascii_letters):
    return ''.join(random.choice(chars) if c.isalpha() else c for c in s)

def randomize_phone_number(phone):
    return ''.join(random.choice(string.digits) if c.isdigit() else c for c in phone)

def randomize_curp_rfc(s):
    return ''.join(
        random.choice(string.ascii_letters) if (c.isalpha() and not (i in range(4, 10))) else c
        for i, c in enumerate(s)
    )
def random_7_digit_number():
    return ''.join(random.choices(string.digits, k=7))
columns_to_randomize = [
    'APPATERNO', 'APMATERNO', 'RAZON_SOCIAL', 'NOMBRE_APODERADO', 'DOMICILIO', 'CORREO'
]

#%%

df = getDataFiles_general_Driver('x','x','PLD_Clientes_Usuarios_306')
df = df.rename(columns={
    'NumCliente': 'NUMCLIENTE',
    'Persona.RazonSocial': 'RAZON_SOCIAL',
    'RelacionContractual': 'RELACION_CONTRATO',
    'Persona.ApellidoPaterno': 'APPATERNO',
    'Persona.ApellidoMaterno': 'APMATERNO',
    'Persona.Nombre': 'NOMBRE',
    'Persona.TipoPersona': 'TIPO_PER',
    'EstatusCliente': 'ESTATUS',
    'FechaInicioRelacion': 'FALTA',
    'ClasificacionGradoRiesgo': 'CLASIF_RIESGO',
    'FechaClasificacionGradoRIesgo': 'FEC_RIESGO',
    'PesId': 'PEP',
    'Nacionalidad': 'NACIONALIDAD',
    'PaisNacimiento': 'PAIS_NAC',
    'ActividadGenerica': 'ACT_GENERICA',
    'ActividadEspecifica': 'ACT_ESPECIFICA',
    'ActividadEspecifica': 'CVE_ENTFED_NAC',
    'NumeroTelefono': 'TELEFONO',
    'Persona.CURP': 'CURP',
    'Persona.RFC': 'RFC',
    'Persona.FechaNacimiento': 'FNACIM',
    'FechaTerminoRelacion': 'FEC_TERMINO',
    'Apoderado': 'NOMBRE_APODERADO',
    'DomicilioParticular': 'DOMICILIO',
    'CorreoElectronico': 'CORREO'
})

print(df.dtypes)
# Drop specified columns from the Dask DataFrame
df= df.drop(['CasfimId' ,'Entidad','IdVisita','PeriodoRequerimiento','Pep','EntidadFederativa','SectorId','_id'], axis=1, columns=None)

start = time.time()



def random_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def randomize_string(s, chars=string.ascii_letters):
    return ''.join(random.choice(chars) if c.isalpha() else c for c in s)

def randomize_phone_number(phone):
    return ''.join(random.choice(string.digits) if c.isdigit() else c for c in phone)

def randomize_curp_rfc(s):
    return ''.join(
        random.choice(string.ascii_letters) if (c.isalpha() and not (i in range(4, 10))) else c
        for i, c in enumerate(s)
    )

def random_7_digit_number():
    return ''.join(random.choices(string.digits, k=7))

columns_to_randomize = [
    'APPATERNO', 'APMATERNO', 'RAZON_SOCIAL', 'NOMBRE_APODERADO', 'DOMICILIO', 'CORREO'
]

def apply_randomization(partition):
    for col in columns_to_randomize:
        partition[col] = partition[col].apply(randomize_string)

    partition['TELEFONO'] = partition['TELEFONO'].apply(randomize_phone_number)
    partition['CURP'] = partition['CURP'].apply(randomize_curp_rfc)
    partition['RFC'] = partition['RFC'].apply(randomize_curp_rfc)

    partition['APPATERNO'] = partition['APPATERNO'].replace(' ', '')
    partition['APMATERNO'] = partition['APMATERNO'].replace(' ', '')
    partition['NOMBRE_APODERADO'] = partition['NOMBRE_APODERADO'].replace(' ', '')

    return partition

# Assuming 'df' is your Dask DataFrame
##df = df.map_partitions(apply_randomization)

# Modificación para la columna 'NUMCLIENTE'
##unique_clients = df['NUMCLIENTE'].unique().compute()
##random_numbers = {client: random_7_digit_number() for client in unique_clients}
##random_numbers = dd.from_pandas(pd.Series(random_numbers), npartitions=1)
##df['NUMCLIENTE'] = df['NUMCLIENTE'].map(lambda x: random_numbers[x], meta=('NUMCLIENTE', 'f8'))

###def random_7_digit_number():
###    return ''.join(random.choices(string.digits, k=7))

###def assign_random_numbers(group):
###    random_number = random_7_digit_number()
###    group['NUMCLIENTE'] = random_number
###    return group

# Group the Dask DataFrame by 'NUMCLIENTE'
###grouped = df.groupby('NUMCLIENTE')

# Apply the 'assign_random_numbers' function to each group and combine the result
###df = grouped.map_partitions(assign_random_numbers, meta=df)

# Compute the result
###df = df.compute()


def apply_randomization(df):
    for col in columns_to_randomize:
        df[col] = df[col].apply(randomize_string)

    df['TELEFONO'] = df['TELEFONO'].apply(randomize_phone_number)
    df['CURP'] = df['CURP'].apply(randomize_curp_rfc)
    df['RFC'] = df['RFC'].apply(randomize_curp_rfc)
    
    df['APPATERNO'] = df['APPATERNO'].replace(' ', '')
    df['APMATERNO'] = df['APMATERNO'].replace(' ', '')
    df['NOMBRE_APODERADO'] = df['NOMBRE_APODERADO'].replace(' ', '')
    
    return df

df = df.map_partitions(apply_randomization)



# Modificación para la columna 'NUMCLIENTE'
###unique_clients = df['NUMCLIENTE'].unique().compute()
###random_numbers = {client: random_7_digit_number() for client in unique_clients}

###def apply_random_numbers(x, random_numbers):
###    return random_numbers.get(x, x)

###df['NUMCLIENTE'] = df['NUMCLIENTE'].map(lambda x: apply_random_numbers(x, random_numbers), meta=('NUMCLIENTE', 'f8'))

print("\n Clientes usuarios random columnas NUMCLIENTE, AAPATERNO, AAMATERNO, RAZON_SOCIAL, CURP, RFC, NOMBRE APODERADO, DOMICILIO, CORREO  PLD_Clients_279_rnd")

output_path = r'E:\temp\mongodb\db\PLD_Cliente_Usuarios_306_rnd.csv'

# Repartition the Dask DataFrame to have only one partition
df = df.repartition(npartitions=1)

# Save the Dask DataFrame to a CSV file
df.to_csv(output_path, index=False, single_file=True)


print(df.compute().head(2))
end = time.time()
print("\n tiempo =",end-start)
# %%
