import sqlalchemy
from sqlalchemy import MetaData
from sqlalchemy.orm import Session
from . import model, schema
from .database import engine


def start_process(db: Session):
    db_process = model.PreprocessEntry()
    db.add(db_process)
    db.commit()
    db.refresh(db_process)
    return db_process


def update(db: Session, preprocess_entry: model.PreprocessEntry):
    meta_data = MetaData()
    meta_data.reflect(bind=engine)
    stmt = (sqlalchemy.update(meta_data.tables["entries"])
            .where("id" == preprocess_entry.id)
            .values({"status": preprocess_entry.status, "output": preprocess_entry.output,
                     "error": preprocess_entry.error}))
    db.execute(stmt)
    db.commit()


def get(db: Session, process_id):
    return db.query(model.PreprocessEntry).get(process_id)


def get_all(db: Session):
    return db.query(model.PreprocessEntry).all()
