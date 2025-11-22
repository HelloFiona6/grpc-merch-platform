import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool


class DBManager:

    def __init__(self):
        # Create the connection pool using environment variables
        self.pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST", "postgres"),
            database=os.getenv("POSTGRES_DB"),
            port=os.getenv("POSTGRES_PORT", "5432")
        )
        if not self.pool:
            raise Exception("Connection pool creation failed!")

    # --------------------
    # Internal helpers
    # --------------------
    def _get_conn(self):
        """Get one connection from pool"""
        return self.pool.getconn()

    def _put_conn(self, conn):
        """Return the connection to pool"""
        self.pool.putconn(conn)

    # --------------------
    # Products CRUD
    # --------------------
    def list_products(self):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, name, category, price, stock
                    FROM products;
                """)
                return cur.fetchall()
        finally:
            self._put_conn(conn)

    def get_product(self, pid):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, name, category, price, stock
                    FROM products
                    WHERE id = %s;
                """, (pid,))
                return cur.fetchone()
        finally:
            self._put_conn(conn)

    # --------------------
    # Users CRUD
    # --------------------
    def create_user(self, username, password_hash):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (username, password_hash)
                    VALUES (%s, %s)
                    RETURNING id, username, active;
                """, (username, password_hash))
                row = cur.fetchone()
                conn.commit()
                return row
        finally:
            self._put_conn(conn)

    def get_user(self, uid):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, username, active
                    FROM users
                    WHERE id = %s;
                """, (uid,))
                return cur.fetchone()
        finally:
            self._put_conn(conn)

    def update_user(self, uid, username, active):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users
                    SET username = %s, active = %s
                    WHERE id = %s
                    RETURNING id, username, active;
                """, (username, active, uid))
                row = cur.fetchone()
                conn.commit()
                return row
        finally:
            self._put_conn(conn)

    # --------------------
    # Orders CRUD
    # --------------------
    def create_order(self, user_id, product_id, quantity):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO orders (user_id, product_id, quantity, total_price)
                    SELECT %s, %s, %s, price * %s
                    FROM products
                    WHERE id = %s
                    RETURNING id, user_id, product_id, quantity, total_price, canceled;
                """, (user_id, product_id, quantity, quantity, product_id))
                row = cur.fetchone()
                conn.commit()
                return row
        finally:
            self._put_conn(conn)

    def get_order(self, order_id):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, user_id, product_id, quantity, total_price, canceled
                    FROM orders
                    WHERE id = %s;
                """, (order_id,))
                return cur.fetchone()
        finally:
            self._put_conn(conn)

    def cancel_order(self, order_id):
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orders
                    SET canceled = TRUE
                    WHERE id = %s
                    RETURNING id, user_id, product_id, quantity, total_price, canceled;
                """, (order_id,))
                row = cur.fetchone()
                conn.commit()
                return row
        finally:
            self._put_conn(conn)

    # --------------------
    # Clean up
    # --------------------
    def close_pool(self):
        """Close all opened connections"""
        self.pool.closeall()
