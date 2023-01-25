#!/usr/bin/python3

"""
    L'edi cannon è uno script master-slave
    Questo è lo script iniziale dell'edi_cannon. E' il Master.
    Si occupa di lanciare i processi slave, uno per ogni azienda presente nelle connessioni_db.
    Lo slave parte con lo script cannoneggiamento_aziendale, funzione main.
"""

import os
import multiprocessing as mp
from multiprocessing.connection import wait
from db_connection import connessioni_db
import cannoneggiamento_aziendale
import signal
import sys
import utils

children = {}
sentinels = {}
parentpid = os.getpid()

def ammazza_tutti(signal_number, stack):
    global children
    if os.getpid() != parentpid:
        pass
    else:
        for process in children.values():
            process.terminate()
    sys.exit(0)


def f(azienda, host, user, password, db):
    print(azienda)
    cannoneggiamento_aziendale.main(azienda, host, user, password, db)
    print("Dopo chiamata main")


def lancia_processo(db):
    global children
    global sentinels
    children[db["azienda"]] = mp.Process(
        target=f,
        args=(db["azienda"], db["host"], db["user"], db["password"], db["database"])
    )
    children[db["azienda"]].daemon = True
    children[db["azienda"]].name = db["azienda"]
    children[db["azienda"]].start()
    sentinels[children[db["azienda"]].sentinel] = children[db["azienda"]]


def rilancia_figlio(azienda):
    db = None
    for conn in connessioni_db:
        if azienda == conn['azienda']:
            db = conn
            break
    if not db:
        return
    lancia_processo(db)


def main():
    global children
    global sentinels

    signal.signal(signal.SIGTERM, ammazza_tutti)
    signal.signal(signal.SIGINT, ammazza_tutti)
    for db in connessioni_db:
        print(db)
        n_process_todo = int(utils.get_numero_thread_and_set_todo(db))
        for i in range(n_process_todo):
            #mi collego al db e guardo quanti processi devo lanciare poi metto lancia processo in un ciclo
            lancia_processo(db)

    while sentinels.keys() or mp.active_children():
        ready = wait(sentinels.keys())
        for k in ready:
            sentinels[k].join()
            figlio_morto = sentinels[k]
            del (sentinels[k])
            print("Il processo per %s è morto, lo rilancio" % figlio_morto.name)
            rilancia_figlio(figlio_morto.name)


if __name__ == '__main__':
    main()
