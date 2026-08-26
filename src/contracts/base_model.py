from sqlalchemy.orm import DeclarativeBase

# This is the base implementation for all the models in the contracts module, it is used to create the tables in the database
# This is pretty common practice in SQLAlchemy to have a base class that all models inherit from, so 
# that they can share common functionality and metadata.
# As I come from Django and Basic Java this would be similar to having an abstract base class 
# that all models inherit from, so that they can share common functionality and metadata.
class Base(DeclarativeBase):
    pass
