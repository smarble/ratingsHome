"""Utility file to seed ratings database from MovieLens data in seed_data/"""

from sqlalchemy import func
from model import User
# from model import Rating
# from model import Movie

from model import connect_to_db, db
from server import app


def load_users():
    """Load users from u.user into database."""
    # Typo? changed from Users to users
    print("users")

    # Delete all rows in table, so if we need to run this a second time,
    # we won't be trying to add duplicate users
    User.query.delete()

    # Read u.user file and insert data
    for row in open("seed_data/u.user"):
        row = row.rstrip()
        # unpack: you must have the same amount of variables as you have items
        # in your source list
        user_id, age, gender, occupation, zipcode = row.split("|")

        # make a new instance/object from the User class, name it user
        user = User(user_id=user_id,
                    age=age,
                    zipcode=zipcode)

        # We need to add to the session or it won't ever be stored
        db.session.add(user)

    # Once we're done, we should commit our work
    db.session.commit()


def load_movies():
    """Load movies from u.item into database."""
    # see below this function for my first, simpler solution. That solution does not address the date/time issue.

    print("movies")

    for i, row in enumerate(open(movie_filename)):
        row = row.rstrip()

        # clever -- we can unpack part of the row!
        movie_id, title, released_str, junk, imdb_url = row.split("|")[:5]

        # The date is in the file as daynum-month_abbreviation-year;
        # we need to convert it to an actual datetime object.

        if released_str:
            released_at = datetime.datetime.strptime(released_str, "%d-%b-%Y")
        else:
            released_at = None

        # Remove the (YEAR) from the end of the title.

        title = title[:-7]   # " (YEAR)" == 7

        movie = Movie(title=title,
                      released_at=released_at,
                      imdb_url=imdb_url)

        # We need to add to the session or it won't ever be stored
        db.session.add(movie)

        # provide some sense of progress
        if i % 100 == 0:
            print(i)

    # Once we're done, we should commit our work
    db.session.commit()

# def load_movies():
#     """Load movies from u.item into database."""
      # This version does not yet convert the date to a date/time object
#     print("movie")

#     # Delete all rows in table, so if we need to run this a second time,
#     # we won't be trying to add duplicate users
#     Movie.query.delete()

#     # Read u.user file and insert data
#     for row in open("seed_data/u.item"):
#         row = row.rstrip()

#         sliced_list = row.split("|")
#         movie_id = sliced_list[0]
#         title = sliced_list[1]
#         released_at = sliced_list[2]
#         imdb_url = sliced_list[3]
#     # in the variable below, we sliced title and removed the last 6 characters

#         title = title[:-6]

#     # another way of slicing the above information movie_id, title,
#     # released_at, imdb_url = row.split("|")

#         movie_info = Movie(movie_id=movie_id,
#                            title=title,
#                            released_at=released_at,
#                            imdb_url=imdb_url)

#         # We need to add to the session or it won't ever be stored
#         db.session.add(movie_info)

#     # Once we're done, we should commit our work
#     db.session.commit()    


def load_ratings():
    """Load ratings from u.data into database."""
    # see below this function for my first, simpler solution

    print("Ratings")

    for i, row in enumerate(open(rating_filename)):
        row = row.rstrip()

        user_id, movie_id, score, timestamp = row.split("\t")

        user_id = int(user_id)
        movie_id = int(movie_id)
        score = int(score)

        # We don't care about the timestamp, so we'll ignore this

        rating = Rating(user_id=user_id,
                        movie_id=movie_id,
                        score=score)

        # We need to add to the session or it won't ever be stored
        db.session.add(rating)

        # provide some sense of progress
        if i % 1000 == 0:
            print(i)

            # An optimization: if we commit after every add, the database
            # will do a lot of work committing each record. However, if we
            # wait until the end, on computers with smaller amounts of
            # memory, it might thrash around. By committing every 1,000th
            # add, we'll strike a good balance.

            db.session.commit()

    # Once we're done, we should commit our work
    db.session.commit()


# def load_ratings():
#     """My first solution see above for second solution. Load ratings from u.data into database."""

#     print("rating")

#     # Delete all rows in table, so if we need to run this a second time,
#     # Use the class name from model.py for the .query.delete() below.
#     Rating.query.delete()

#     # Read u.user file and insert data
#     for row in open("seed_data/u.data"):
#         row = row.rstrip()
#         rating_id, movie_id, user_id, score = row.split(" ")

#         rating_info = Rating(rating_id=rating_id,
#                              movie_id=movie_id,
#                              user_id=user_id,
#                              score=score)

#         # We need to add to the session or it won't ever be stored
#         db.session.add(rating_info)

#     # Once we're done, we should commit our work
#     db.session.commit()
   



def set_val_user_id():
    """Set value for the next user_id after seeding database"""
    # In our imported data, users already have id numbers. When Postgress tries 
    # to start it's "sequence" at 1, it will error. 
    # Get the Max user_id in the database
    result = db.session.query(func.max(User.user_id)).one()
    max_id = int(result[0])

    # Set the value for the next user_id to be max_id + 1
    query = "SELECT setval('users_user_id_seq', :new_id)"
    db.session.execute(query, {'new_id': max_id + 1})
    db.session.commit()


if __name__ == "__main__":
    connect_to_db(app)

    # In case tables haven't been created, create them
    db.create_all()

    # Import different types of data
    load_users()
    load_movies()
    load_ratings()
    set_val_user_id()
