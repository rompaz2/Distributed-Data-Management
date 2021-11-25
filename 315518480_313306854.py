import pyodbc
import datetime
import os
import pandas as pd
import multiprocessing


X = 23
Y = 13
Z = 54


def main():
    manage_transaction(20)


def connect_to_DB(dbname):
    cnxn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=technionddscourse.database.windows.net;'
                          f'DATABASE={dbname};'
                          f'UID={dbname};'
                          'PWD=Qwerty12!')
    return cnxn


def is_lock_free(tID, pID, r_w, cnxn):
    cursor = cnxn.cursor()
    if r_w == 'read':
        query = f"select * from Locks L where L.productID={pID} and L.lockType='{r_w}'"
    else:
        query = f"select * from Locks L where L.productID={pID} and L.transactionID<>'{tID}'"
    commit_log('Locks', 'read', tID, pID, cnxn, query)
    cursor.execute(query)
    row = cursor.fetchone()
    if row:
        return False
    else:
        return True


def get_lock(tID, pID, r_w, cnxn):
    cursor = cnxn.cursor()
    if is_lock_free(tID, pID, r_w, cnxn):
        query = f"INSERT INTO Locks(transactionID, productID, lockType) VALUES('{tID}', {pID}, '{r_w}') "
        commit_log('Locks', 'insert', tID, pID, cnxn, query)
        cursor.execute(query)
        cursor.commit()
        return True
    else:
        return False


def release_lock(tID, pID, r_w, cnxn):
    cursor = cnxn.cursor()
    query = f"DELETE FROM Locks WHERE productID={pID} AND lockType='{r_w}' AND transactionID='{tID}' "
    commit_log('Locks', 'delete', tID, pID, cnxn, query)
    cursor.execute(query)
    cursor.commit()


def commit_log(rID, action, tID, pID, cnxn, sql_query):
    cursor = cnxn.cursor()
    timestamp = datetime.datetime.now()
    cursor.execute(f"INSERT INTO Log(timestamp, relation, transactionID, productID, action, record) "
                   f"   VALUES(?, ?, ?, ?, ?, ?)", timestamp, rID, tID, pID, action, sql_query)


def manage_transaction(T):
    delta = T / 15
    dbteam_cnxn = connect_to_DB('dbteam')
    dbteam_cursor = dbteam_cnxn.cursor()
    dbteam_cursor.execute("SELECT * FROM CategoriesToSites")
    rows = dbteam_cursor.fetchall()
    cat_dbname = {}
    for row in rows:
        cat_dbname[row[0]] = row[1]
    t_list = sorted(os.listdir('orders'))
    success_failed = []
    for t in t_list:
        queue = multiprocessing.Queue()
        p = multiprocessing.Process(target=run_transaction, args=(t, cat_dbname, queue))
        p.start()
        p.join(T - delta)
        return_dict = queue.get()
        if p.is_alive():
            p.terminate()
            state = 'failed'
            if return_dict['state'] == 'commit':
                cats_to_finish = set(return_dict['pre_commit']) - set(return_dict['committed_list'])
                for cat in cats_to_finish:
                    cnxn = connect_to_DB(cat_dbname[cat])
                    run_order_safely(return_dict['cats_product_amount'][cat], t[:-4], cnxn)
                state = 'succeeded'
            success_failed.append(state)
        else:
            success_failed.append(return_dict['success_failed'])
        for cat in return_dict['locked_cat']:
            cnxn = connect_to_DB(cat_dbname[cat])
            for pID in return_dict['cats_product_amount'][cat]:
                release_lock(t[:-4], pID, 'write', cnxn)
    for i in range(len(t_list)):
        print(f"Transaction {t_list[i][:-4]} {success_failed[i]}.")


def run_transaction(t, cat_dbname, queue):
    return_dict = \
        {'cats_product_amount': {}, 'success_failed': 'succeeded', 'locked_cat': set(),
         'pre_commit': [], 'committed_list': [], 'state': 'initial'}
    queue.put(return_dict)
    path = f"orders/" + t
    df = pd.read_csv(path, header=0)
    cats_to_update = df['categoryID'].unique()
    cat_cnxn = {}
    for cat in cats_to_update:
        cat_cnxn[cat] = connect_to_DB(cat_dbname[cat])
    cats_product_amount = \
        {cat: {pID: amount['amount'].item()
               for pID, amount in df[df['categoryID'] == cat].groupby('productID')}
         for cat in cats_to_update}
    queue.get()
    return_dict['cats_product_amount'] = cats_product_amount
    queue.put(return_dict)
    tID = t[:-4]
    got_all_locks = True
    for cat, items in cats_product_amount.items():
        for pID in items.keys():
            if get_lock(tID, pID, 'write', cat_cnxn[cat]):
                queue.get()
                return_dict['locked_cat'].add(cat)
                queue.put(return_dict)
                continue
            else:
                got_all_locks = False
                queue.get()
                return_dict['success_failed'] = 'failed'
                queue.put(return_dict)
    if got_all_locks:
        for cat_to_update in cats_to_update:
            ack = run_order(cats_product_amount[cat_to_update], tID, cat_cnxn[cat_to_update])
            if ack == 'commit':
                queue.get()
                return_dict['pre_commit'].append(cat_to_update)
                queue.put(return_dict)
            else:
                global_abort(return_dict['pre_commit'], cat_cnxn)
                queue.get()
                return_dict['success_failed'] = 'failed'
                queue.put(return_dict)
        queue.get()
        return_dict['state'] = 'commit'
        queue.put(return_dict)
        global_commit(queue, cat_cnxn)


def run_order(product_amount, tID, cnxn):
    cursor = cnxn.cursor()
    for pID, amount in product_amount.items():
        query = f"select inventory from productsInventory PI where PI.productID={pID} AND inventory >= {amount}"
        commit_log('productsInventory', 'read', tID, pID, cnxn, query)
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            query = f"update ProductsInventory set inventory={row[0]-amount} where productID={pID}"
            commit_log('productsInventory', 'update', tID, pID, cnxn, query)
            cursor.execute(query)
            query = f"insert into ProductsOrdered(transactionID, productID, amount) values('{tID}',{pID},{amount})"
            commit_log('ProductsOrdered', 'insert', tID, pID, cnxn, query)
            cursor.execute(query)
        else:
            cursor.rollback()
            return 'abort'
    return 'commit'


def run_order_safely(product_amount, tID, cnxn):
    cursor = cnxn.cursor()
    for pID, amount in product_amount.items():
        query = f"select inventory from productsInventory PI where PI.productID={pID}"
        commit_log('productsInventory', 'read', tID, pID, cnxn, query)
        cursor.execute(query)
        row = cursor.fetchone()
        query = f"update ProductsInventory set inventory={row[0] - amount} where productID={pID}"
        commit_log('productsInventory', 'update', tID, pID, cnxn, query)
        cursor.execute(query)
        query = f"insert into ProductsOrdered(transactionID, productID, amount) values('{tID}',{pID},{amount})"
        commit_log('ProductsOrdered', 'insert', tID, pID, cnxn, query)
        cursor.execute(query)
    cursor.commit()


def global_abort(pre_commit_list, cat_cnxn):
    for cat in pre_commit_list:
        cat_cnxn[cat].rollback()


def global_commit(queue, cat_cnxn):
    return_dict = queue.get()
    queue.put(return_dict)
    for cat in return_dict['pre_commit']:
        queue.get()
        cat_cnxn[cat].commit()
        return_dict['committed_list'].append(cat)
        queue.put(return_dict)
    queue.get()
    return_dict['state'] = 'finished'
    queue.put(return_dict)


def update_inventory(transactionID):
    cnxn = connect_to_DB('rompaz')
    cursor = cnxn.cursor()
    amount = 48
    for pID in range(1, Y + 1):
        if pID == 2:
            amount = 41
        query1 = f"select * from productsInventory PI where PI.productID={pID}"
        cursor.execute(query1)
        row = cursor.fetchone()
        if not row:
            query2 = f"insert into ProductsInventory(productID, inventory) values({pID},{amount})"
            cursor.execute(query2)
            commit_log('productsInventory', 'read', transactionID, pID, cnxn, query1)
            commit_log('productsInventory', 'insert', transactionID, pID, cnxn, query2)
            get_lock(transactionID, pID, 'write', cnxn)
        else:
            commit_log('productsInventory', 'read', transactionID, pID, cnxn, query1)
            if get_lock(transactionID, pID, 'write', cnxn):
                query2 = f"update ProductsInventory set inventory={amount} where productID={pID}"
                commit_log('productsInventory', 'update', transactionID, pID, cnxn, query2)
                cursor.execute(query2)
            else:
                cursor.rollback()
                break
    for pID in range(1, Y + 1):
        release_lock(transactionID, pID, 'write', cnxn)
    cursor.commit()


def create_tables():
    cnxn = connect_to_DB('rompaz')
    cursor = cnxn.cursor()
    cursor.execute("CREATE TABLE ProductsInventory"
                   "(productID INT PRIMARY KEY,"
                   "inventory INT"
                   "    CHECK (inventory >= 0)"
                   ")")
    cursor.execute("CREATE TABLE ProductsOrdered"
                   "(transactionID VARCHAR(30),"
                   "productID INT,"
                   "amount INT,"
                   "    CHECK (amount >= 1),"
                   "PRIMARY KEY (transactionID, productID),"
                   "FOREIGN KEY (productID) REFERENCES ProductsInventory ON DELETE CASCADE"
                   ")")
    cursor.execute("CREATE TABLE Log"
                   "(rowID INT IDENTITY(1, 1) PRIMARY KEY,"
                   "timestamp DATETIME,"
                   "relation VARCHAR(17) "
                   "    CHECK (relation='ProductsInventory' or relation='Locks' or relation='ProductsOrdered'),"
                   "transactionID VARCHAR(30),"
                   "productID INT,"
                   "action VARCHAR(6) "
                   "    CHECK (action='read' or action='update' or action='delete' or action='insert'),"
                   "record VARCHAR(2500),"
                   "FOREIGN KEY (productID) REFERENCES ProductsInventory ON DELETE CASCADE"
                   ")")
    cursor.execute("CREATE TABLE Locks"
                   "(transactionID VARCHAR(30),"
                   "productID INT,"
                   "lockType VARCHAR(5)"
                   "    CHECK (lockType='read' or lockType='write'),"
                   "PRIMARY KEY (transactionID, productID, lockType),"
                   "FOREIGN KEY (productID) REFERENCES ProductsInventory ON DELETE CASCADE"
                   ")")
    cursor.commit()


if __name__ == '__main__':
    main()