import schedule
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
import logging

# Configure logging
logger = logging.getLogger('cdi.log')
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('cdi.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Database connection parameters
db_name = 'se4g'
user = 'postgres'
password = '666888'
host = 'localhost'

# IdroGEO API endpoint
IdroGEOAPI = 'https://test.idrogeo.isprambiente.it/api/pir'
regionAPI = IdroGEOAPI + "/regioni"


def fetch_data():
    try:
        conn = psycopg2.connect(dbname=db_name, user=user, password=password, host=host)
        cursor = conn.cursor()
        search_query = "SELECT uid, nome FROM regioni"
        cursor.execute(search_query)
        results = cursor.fetchall()

        uid = results[0][0]
        regionIdAPI = regionAPI + "/" + str(uid)
        response = requests.get(regionIdAPI)
        response.raise_for_status()
        logger.info("Data fetched successfully from the database.")
        cursor.close()
        conn.close()
        return results
    except requests.RequestException as e:
        logger.error(f"Error fetching data from database: {e}")
        return None


def process_data(raw_data):
    try:
        flood_data = []
        for i in raw_data:
            uid = i[0]
            nome = i[1]
            regionIdAPI = regionAPI + "/" + str(uid)
            response = requests.get(regionIdAPI)
            if response.status_code == 200:
                res = response.json()
                if nome == res['nome']:
                    flood_item = {
                        'uid': res.get('uid'),
                        'osmid': res.get('osmid'),
                        'nome': res.get('nome'),
                        'ar_kmq': res.get('ar_kmq'),
                        'ar_id_p3': res.get('ar_id_p3'),
                        'ar_id_p2': res.get('ar_id_p2'),
                        'ar_id_p1': res.get('ar_id_p1'),
                        'aridp3_p': res.get('aridp3_p'),
                        'aridp2_p': res.get('aridp2_p'),
                        'aridp1_p': res.get('aridp1_p'),
                        'pop_res011': res.get('pop_res011'),
                        'pop_gio': res.get('pop_gio'),
                        'pop_gio_p': res.get('pop_gio_p'),
                        'pop_adu': res.get('pop_adu'),
                        'pop_adu_p': res.get('pop_adu_p'),
                        'pop_anz': res.get('pop_anz'),
                        'pop_anz_p': res.get('pop_anz_p'),
                        'pop_idr_p3': res.get('pop_idr_p3'),
                        'pop_idr_p2': res.get('pop_idr_p2'),
                        'pop_idr_p1': res.get('pop_idr_p1'),
                        'popidp3_p': res.get('popidp3_p'),
                        'popidp2_p': res.get('popidp2_p'),
                        'popidp1_p': res.get('popidp1_p'),
                        'fam_tot': res.get('fam_tot'),
                        'fam_idr_p3': res.get('fam_idr_p3'),
                        'fam_idr_p2': res.get('fam_idr_p2'),
                        'fam_idr_p1': res.get('fam_idr_p1'),
                        'famidp3_p': res.get('famidp3_p'),
                        'famidp2_p': res.get('famidp2_p'),
                        'famidp1_p': res.get('famidp1_p'),
                        'ed_tot': res.get('ed_tot'),
                        'ed_idr_p3': res.get('ed_idr_p3'),
                        'ed_idr_p2': res.get('ed_idr_p2'),
                        'ed_idr_p1': res.get('ed_idr_p1'),
                        'edidp3_p': res.get('edidp3_p'),
                        'edidp2_p': res.get('edidp2_p'),
                        'edidp1_p': res.get('edidp1_p'),
                        'im_tot': res.get('im_tot'),
                        'im_idr_p3': res.get('im_idr_p3'),
                        'im_idr_p2': res.get('im_idr_p2'),
                        'im_idr_p1': res.get('im_idr_p1'),
                        'imidp3_p': res.get('imidp3_p'),
                        'imidp2_p': res.get('imidp2_p'),
                        'imidp1_p': res.get('imidp1_p'),
                        'n_vir': res.get('n_vir'),
                        'bbcc_id_p3': res.get('bbcc_id_p3'),
                        'bbcc_id_p2': res.get('bbcc_id_p2'),
                        'bbcc_id_p1': res.get('bbcc_id_p1'),
                        'bbccidp3_p': res.get('bbccidp3_p'),
                        'bbccidp2_p': res.get('bbccidp2_p'),
                        'bbccidp1_p': res.get('bbccidp1_p')
                    }
            else:
                raise response.status_code
            flood_data.append(flood_item)
        logger.info("Flood Data processed successfully.")
        return flood_data
    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return None


def integrate_data(processed_data):
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=db_name,
            user=user,
            password=password
        )
        cursor = conn.cursor()

        # Define the SQL query to insert/update data
        sql = """
                INSERT INTO regioni_flood (
                    uid, osmid, nome, ar_kmq, ar_id_p3, ar_id_p2, ar_id_p1,
                    aridp3_p, aridp2_p, aridp1_p, pop_res011, pop_gio,
                    pop_gio_p, pop_adu, pop_adu_p, pop_anz, pop_anz_p,
                    pop_idr_p3, pop_idr_p2, pop_idr_p1, popidp3_p, popidp2_p,
                    popidp1_p, fam_tot, fam_idr_p3, fam_idr_p2, fam_idr_p1,
                    famidp3_p, famidp2_p, famidp1_p, ed_tot, ed_idr_p3,
                    ed_idr_p2, ed_idr_p1, edidp3_p, edidp2_p, edidp1_p,
                    im_tot, im_idr_p3, im_idr_p2, im_idr_p1, imidp3_p,
                    imidp2_p, imidp1_p, n_vir, bbcc_id_p3, bbcc_id_p2,
                    bbcc_id_p1, bbccidp3_p, bbccidp2_p, bbccidp1_p
                ) VALUES %s
                ON CONFLICT (uid) DO UPDATE SET
                    osmid = EXCLUDED.osmid,
                    nome = EXCLUDED.nome,
                    ar_kmq = EXCLUDED.ar_kmq,
                    ar_id_p3 = EXCLUDED.ar_id_p3,
                    ar_id_p2 = EXCLUDED.ar_id_p2,
                    ar_id_p1 = EXCLUDED.ar_id_p1,
                    aridp3_p = EXCLUDED.aridp3_p,
                    aridp2_p = EXCLUDED.aridp2_p,
                    aridp1_p = EXCLUDED.aridp1_p,
                    pop_res011 = EXCLUDED.pop_res011,
                    pop_gio = EXCLUDED.pop_gio,
                    pop_gio_p = EXCLUDED.pop_gio_p,
                    pop_adu = EXCLUDED.pop_adu,
                    pop_adu_p = EXCLUDED.pop_adu_p,
                    pop_anz = EXCLUDED.pop_anz,
                    pop_anz_p = EXCLUDED.pop_anz_p,
                    pop_idr_p3 = EXCLUDED.pop_idr_p3,
                    pop_idr_p2 = EXCLUDED.pop_idr_p2,
                    pop_idr_p1 = EXCLUDED.pop_idr_p1,
                    popidp3_p = EXCLUDED.popidp3_p,
                    popidp2_p = EXCLUDED.popidp2_p,
                    popidp1_p = EXCLUDED.popidp1_p,
                    fam_tot = EXCLUDED.fam_tot,
                    fam_idr_p3 = EXCLUDED.fam_idr_p3,
                    fam_idr_p2 = EXCLUDED.fam_idr_p2,
                    fam_idr_p1 = EXCLUDED.fam_idr_p1,
                    famidp3_p = EXCLUDED.famidp3_p,
                    famidp2_p = EXCLUDED.famidp2_p,
                    famidp1_p = EXCLUDED.famidp1_p,
                    ed_tot = EXCLUDED.ed_tot,
                    ed_idr_p3 = EXCLUDED.ed_idr_p3,
                    ed_idr_p2 = EXCLUDED.ed_idr_p2,
                    ed_idr_p1 = EXCLUDED.ed_idr_p1,
                    edidp3_p = EXCLUDED.edidp3_p,
                    edidp2_p = EXCLUDED.edidp2_p,
                    edidp1_p = EXCLUDED.edidp1_p,
                    im_tot = EXCLUDED.im_tot,
                    im_idr_p3 = EXCLUDED.im_idr_p3,
                    im_idr_p2 = EXCLUDED.im_idr_p2,
                    im_idr_p1 = EXCLUDED.im_idr_p1,
                    imidp3_p = EXCLUDED.imidp3_p,
                    imidp2_p = EXCLUDED.imidp2_p,
                    imidp1_p = EXCLUDED.imidp1_p,
                    n_vir = EXCLUDED.n_vir,
                    bbcc_id_p3 = EXCLUDED.bbcc_id_p3,
                    bbcc_id_p2 = EXCLUDED.bbcc_id_p2,
                    bbcc_id_p1 = EXCLUDED.bbcc_id_p1,
                    bbccidp3_p = EXCLUDED.bbccidp3_p,
                    bbccidp2_p = EXCLUDED.bbccidp2_p,
                    bbccidp1_p = EXCLUDED.bbccidp1_p;
                """

        # Convert processed data into a list of tuples for execute_values
        data_tuples = [(d['uid'], d['osmid'], d['nome'], d['ar_kmq'], d['ar_id_p3'], d['ar_id_p2'], d['ar_id_p1'],
                        d['aridp3_p'], d['aridp2_p'], d['aridp1_p'], d['pop_res011'], d['pop_gio'],
                        d['pop_gio_p'], d['pop_adu'], d['pop_adu_p'], d['pop_anz'], d['pop_anz_p'],
                        d['pop_idr_p3'], d['pop_idr_p2'], d['pop_idr_p1'], d['popidp3_p'], d['popidp2_p'],
                        d['popidp1_p'], d['fam_tot'], d['fam_idr_p3'], d['fam_idr_p2'], d['fam_idr_p1'],
                        d['famidp3_p'], d['famidp2_p'], d['famidp1_p'], d['ed_tot'], d['ed_idr_p3'],
                        d['ed_idr_p2'], d['ed_idr_p1'], d['edidp3_p'], d['edidp2_p'], d['edidp1_p'],
                        d['im_tot'], d['im_idr_p3'], d['im_idr_p2'], d['im_idr_p1'], d['imidp3_p'],
                        d['imidp2_p'], d['imidp1_p'], d['n_vir'], d['bbcc_id_p3'], d['bbcc_id_p2'],
                        d['bbcc_id_p1'], d['bbccidp3_p'], d['bbccidp2_p'], d['bbccidp1_p']) for d in processed_data]

        # Execute the SQL query with the processed data
        execute_values(cursor, sql, data_tuples)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Flood data integrated successfully into the database.")
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
    except Exception as e:
        logger.error(f"Error integrating data: {e}")


def job():
    raw_data = fetch_data()
    processed_data = process_data(raw_data)
    integrate_data(processed_data)


job()

# Schedule the job to run at every hour
schedule.every().minute.do(job)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(1)
