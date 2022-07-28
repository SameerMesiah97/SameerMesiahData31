import app.requesting_sw

if __name__ == '__main__':

    db = app.requesting_sw.set_up_db()
    app.requesting_sw.dl_transform(db)
    app.requesting_sw.read_from_db(db)
