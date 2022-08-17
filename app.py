from flask import Flask, request, jsonify, json
import requests
import mysql.connector
import pandas as pd
from scipy import spatial
import operator

app = Flask(__name__)

class Database:
    def __init__(self, host, user, password, db):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.connection = None
        self.cursor = None


    def connect(self):
        self.connection = mysql.connector.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.db
        )
        self.cursor = self.connection.cursor()


    def disconnect(self):
        self.cursor.close()
        self.connection.close()


    def query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

db = Database("localhost", "root", "", "chronics_2")
db.connect()

users =db.query("""
select
customer.id,
GROUP_CONCAT( book.id),
GROUP_CONCAT( book.title)
from customer
left join customer_book on customer.id = customer_book.customer_id
join book on customer_book.book_id = book.id
GROUP BY customer.id
""")
books=db.query("""
select
book.id,
book.title,
author.name,
book.synopsis,
book.created_at,
book.rating,
GROUP_CONCAT( genre.name )

from book
join author on book.author_id = author.id
join book_genre on book.id = book_genre.book_id
join genre on book_genre.genre_id= genre.id

GROUP BY book.id
""")

user_fav=db.query("""
select
favourite.id,
favourite.customer_id,
book_genre.genre_id

from favourite
join book_genre on favourite.genre_id = book_genre.genre_id
""")

user_last_read=db.query("""
select
customer_id,
book_id,
Max(created_at) as Date 

from reading_history
group by customer_id

""")

df_user = pd.DataFrame(users)
df_user.columns=['id','books','title']

df = pd.DataFrame(books)
df.columns=['id','title', 'author','description','created_at','rating','genres']

df_fav = pd.DataFrame(user_fav)
df_fav.columns = ['id','customer_id','genre_id']

df_user_last = pd.DataFrame(user_last_read)
df_user_last.columns = ['id','books','created_at']

print(df_user_last)

#df operations

df['genres'] = df['genres'].astype(str).apply(lambda x: x.split(','))
df['genres'].str.lower()

genresList = []
for index, row in df.iterrows():
    genres = row["genres"]

    for genre in genres:
        if genre not in genresList:
            genresList.append(genre)


def binary_availability(genre_list):
    binaryList = []

    for availability in genresList:
        if availability in genre_list:
            binaryList.append(1)
        else:
            binaryList.append(0)

    return binaryList

df['genres_bin'] = df['genres'].apply(lambda x: binary_availability(x))

@app.route('/get_books_by_id', methods=['GET'])

def get_user():
    userId = int(request.args.get('id'))
    new_user = df_user[df_user['id'] == userId].iloc[0].to_frame().T
    books = new_user.books.values[0].split(',')
    return books


@app.route('/get_similar_books', methods=['GET'])

def get_similar_books_all_read():
    id=int(request.args.get('id'))
    param = int(request.args.get('param'))
    num_books =int(request.args.get('num_books'))
    all_books = []
    new_user = df_user[df_user['id'] == id].iloc[0].to_frame().T

    if pd.isnull(new_user.books.values[0]) == True:
        book_genre = df_fav[df_fav['customer_id'] == id].iloc[0].to_frame().T
        books = book_genre.genre_id.values[0]
        print(books)
        query = """select book_id from book_genre WHERE genre_id = '{books}'""".format(books=books)
        rows = db.query(query)
        result = [{"id": item[0]} for item in rows]

        return json.dumps(result, default=str)
    else:

        books = new_user.books.values[0].split(',')
        books = [int(item) for item in books]

        for id in books:
            books = df[df['id'] == id].iloc[0].to_frame().T

            def Similarity(bookId1, bookId2):

                bookId1 = df[df["id"] == bookId1].index.values[0]
                bookId2 = df[df["id"] == bookId2].index.values[0]

                a = df.iloc[bookId1]
                b = df.iloc[bookId2]

                genreA = a['genres_bin']
                genreB = b['genres_bin']
                genre = spatial.distance.cosine(genreA, genreB)

                return genre

            def getNeighbors(baseUser, K):
                distances = []

                for index, user in df.iterrows():
                    if user['id'] != baseUser['id'].values[0]:
                        dist = Similarity(baseUser['id'].values[0], user['id'])
                        distances.append((user['id'], dist))

                distances.sort(key=operator.itemgetter(1))
                neighbors = []

                for x in range(K):
                    neighbors.append(distances[x])
                return neighbors

            K = param

            neighbors = getNeighbors(books, K)
            close = []
            for neighbor in neighbors:
                if neighbor[1] < 2.5:
                    close.append(neighbor[0])

            all_books = all_books + close
        all_books = list(set(all_books))
        all_books = all_books[0: int(num_books)]

        return jsonify({"results": all_books}), 401


@app.route('/get_books_by_last_read', methods=['GET'])

def get_similar_books_last_read():
    id = int(request.args.get('id'))
    param = int(request.args.get('param'))
    #new_user = df_user_last[df_user_last['id'] == id].iloc[0].to_frame().T
    print(id)

    if id not in df_user_last['id'].values:
        book_genre = df_fav[df_fav['customer_id'] == id].iloc[0].to_frame().T
        books = book_genre.genre_id.values[0]
        query = """select book_id from book_genre WHERE genre_id = '{books}'""".format(books=books)
        rows = db.query(query)
        result = [{"id": item[0]} for item in rows]
        result=result[0: int(param)]

        return json.dumps(result, default=str)
    else:

        new_user = df_user_last[df_user_last['id'] == id].iloc[0].to_frame().T
        books = df[df['id'] == id].iloc[0].to_frame().T

        def Similarity(bookId1, bookId2):

            bookId1 = df[df["id"] == bookId1].index.values[0]
            bookId2 = df[df["id"] == bookId2].index.values[0]

            a = df.iloc[bookId1]
            b = df.iloc[bookId2]

            genreA = a['genres_bin']
            genreB = b['genres_bin']
            genre = spatial.distance.cosine(genreA, genreB)

            return genre

        def getNeighbors(baseUser, K):
            distances = []

            for index, user in df.iterrows():
                if user['id'] != baseUser['id'].values[0]:
                    dist = Similarity(baseUser['id'].values[0], user['id'])
                    distances.append((user['id'], dist))

            distances.sort(key=operator.itemgetter(1))
            neighbors = []

            for x in range(K):
                neighbors.append(distances[x])
            return neighbors

        K = param

        neighbors = getNeighbors(books, K)
        close = []
        for neighbor in neighbors:
            if neighbor[1] < 2.5:
                close.append(neighbor[0])



        return jsonify({"results": close}), 401



@app.route('/get_books_by_genre', methods=['GET'])

def get_by_genres():

    userId = int(request.args.get('id'))
    book_genre = df_fav[df_fav['customer_id'] == userId].iloc[0].to_frame().T
    books = book_genre.genre_id.values[0]
    print(books)
    query = """select book_id from book_genre WHERE genre_id = '{books}'""".format(books=books)
    rows = db.query(query)
    result = [{"id": item[0]} for item in rows]

    return json.dumps(result, default=str)


@app.route('/get_books_by_popular', methods=['GET'])
def get_by_popularity():
    df['rating'].sort_values(by=['rating'], inplace=True)


if __name__ == '__main__':
    app.run()
