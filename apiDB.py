# -*- coding: utf-8 -*-
import os
import sqlite3
import datetime as dt
import csv
import io
from contextlib import contextmanager

from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager, create_access_token, jwt_required, get_jwt_identity
)
from werkzeug.security import generate_password_hash, check_password_hash

# ==========================================================
# CONFIG
# ==========================================================
BASE_DATOS_RUTA = "./"
DB_PATH = os.path.join(BASE_DATOS_RUTA, "sistema_preventivo.db")

SECRET_KEY_JWT = os.getenv("JWT_SECRET", "clave_secreta_para_motores_2026")
API_KEY_TELEMETRIA = os.getenv("API_KEY_TELEMETRIA", "SISTEMA_LORA_SECRET_2026")

ACCESS_TOKEN_HOURS = int(os.getenv("JWT_HOURS", "24"))

app = Flask(__name__)
CORS(app)

app.config["JWT_SECRET_KEY"] = SECRET_KEY_JWT
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = dt.timedelta(hours=ACCESS_TOKEN_HOURS)
jwt = JWTManager(app)

init_db()

# ==========================================================
# DB utils
# ==========================================================
def utcnow_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def parse_iso_datetime(value: str) -> dt.datetime:
    """
    Acepta:
      - '2026-02-16T12:00:00'
      - '2026-02-16T12:00:00Z'
      - '2026-02-16 12:00:00'
    Retorna datetime naive en UTC (asumiendo UTC si viene con Z).
    """
    if not value:
        raise ValueError("datetime vacío")

    v = value.strip().replace(" ", "T")
    if v.endswith("Z"):
        v = v[:-1]
    # fromisoformat no soporta 'Z' directo, ya lo removimos.
    return dt.datetime.fromisoformat(v)

def row_to_dict(row: sqlite3.Row) -> dict:
    return dict(row) if row is not None else None

@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with db_conn() as conn:
        c = conn.cursor()

        # --------------------------
        # AUTH / USERS
        # --------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            creado_en TEXT NOT NULL DEFAULT (datetime('now')),
            activo INTEGER NOT NULL DEFAULT 1
        );
        """)

        # --------------------------
        # HIERARCHY (SOFT DELETE)
        # --------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS plantas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            nombre TEXT NOT NULL,
            ubicacion TEXT,
            email_notificaciones TEXT,
            creado_en TEXT NOT NULL DEFAULT (datetime('now')),
            activo INTEGER NOT NULL DEFAULT 1,
            eliminado_en TEXT,
            FOREIGN KEY(usuario_id) REFERENCES usuarios(id)
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS motores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            planta_id INTEGER NOT NULL,
            codigo TEXT NOT NULL,
            modelo TEXT,
            ubicacion TEXT,
            descripcion TEXT,
            num_anillos INTEGER,
            carbones_por_anillo INTEGER,
            alto_carbon_mm REAL,
            prealarma_mm REAL,
            minimo_cambio_mm REAL,
            umbral_desgaste_perc REAL,
            duracion_estimada_dias INTEGER,
            creado_en TEXT NOT NULL DEFAULT (datetime('now')),
            activo INTEGER NOT NULL DEFAULT 1,
            eliminado_en TEXT,
            UNIQUE(planta_id, codigo),
            FOREIGN KEY(planta_id) REFERENCES plantas(id)
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS anillos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            motor_id INTEGER NOT NULL,
            numero_anillo INTEGER NOT NULL,
            creado_en TEXT NOT NULL DEFAULT (datetime('now')),
            activo INTEGER NOT NULL DEFAULT 1,
            eliminado_en TEXT,
            UNIQUE(motor_id, numero_anillo),
            FOREIGN KEY(motor_id) REFERENCES motores(id)
        );
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS carbones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            anillo_id INTEGER NOT NULL,
            numero_carbon INTEGER NOT NULL,
            medida_inicial_mm REAL NOT NULL,
            umbral_alerta_perc REAL NOT NULL,
            duracion_estimada_dias INTEGER,
            creado_en TEXT NOT NULL DEFAULT (datetime('now')),
            activo INTEGER NOT NULL DEFAULT 1,
            eliminado_en TEXT,
            UNIQUE(anillo_id, numero_carbon),
            FOREIGN KEY(anillo_id) REFERENCES anillos(id)
        );
        """)

        # --------------------------
        # NODES (NO DELETE OF DATA)
        # --------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS nodos (
            deveui TEXT PRIMARY KEY,
            alias TEXT,
            bateria INTEGER DEFAULT 100,
            estado TEXT NOT NULL DEFAULT 'disponible', -- disponible|ocupado|inactivo
            creado_en TEXT NOT NULL DEFAULT (datetime('now')),
            activo INTEGER NOT NULL DEFAULT 1
        );
        """)

        # --------------------------
        # ASSIGNMENT HISTORY
        # Un carbón puede cambiar de nodo, se registra historial.
        # --------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS asignaciones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            carbon_id INTEGER NOT NULL,
            deveui TEXT NOT NULL,
            fecha_inicio TEXT NOT NULL,
            fecha_fin TEXT, -- NULL = vigente
            creado_en TEXT NOT NULL DEFAULT (datetime('now')),
            activo INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY(carbon_id) REFERENCES carbones(id),
            FOREIGN KEY(deveui) REFERENCES nodos(deveui)
        );
        """)

        c.execute("CREATE INDEX IF NOT EXISTS idx_asig_carbon_vigente ON asignaciones(carbon_id, fecha_fin);")
        c.execute("CREATE INDEX IF NOT EXISTS idx_asig_deveui_inicio ON asignaciones(deveui, fecha_inicio);")

        # --------------------------
        # TELEMETRY (NEVER DELETED)
        # --------------------------
        c.execute("""
        CREATE TABLE IF NOT EXISTS telemetria (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deveui TEXT NOT NULL,
            ax REAL, ay REAL, az REAL,
            gx REAL, gy REAL, gz REAL,
            desgaste_calculado REAL,
            bateria INTEGER,
            fecha TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(deveui) REFERENCES nodos(deveui)
        );
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_tel_deveui_fecha ON telemetria(deveui, fecha);")

        # Seed admin (si no existe)
        admin_email = "admin@minera.cl"
        admin_pass = "admin123"
        exists = conn.execute("SELECT 1 FROM usuarios WHERE email=?", (admin_email,)).fetchone()
        if not exists:
            conn.execute(
                "INSERT INTO usuarios (email, password_hash) VALUES (?,?)",
                (admin_email, generate_password_hash(admin_pass))
            )

# ==========================================================
# ERROR HANDLERS (PRODUCTION FRIENDLY)
# ==========================================================
def api_error(status_code: int, message: str, details=None):
    payload = {"error": message}
    if details is not None:
        payload["details"] = details
    return jsonify(payload), status_code

@app.errorhandler(404)
def not_found(_):
    return api_error(404, "Ruta no existe")

@app.errorhandler(405)
def method_not_allowed(_):
    return api_error(405, "Método no permitido")

@app.errorhandler(400)
def bad_request(_):
    return api_error(400, "Solicitud inválida")

@app.errorhandler(500)
def internal_error(_):
    return api_error(500, "Error interno")

# ==========================================================
# AUTH HELPERS
# ==========================================================
def require_json():
    if not request.is_json:
        return False
    return True

def current_user_id() -> int:
    return int(get_jwt_identity())

def user_exists(conn, user_id: int) -> bool:
    r = conn.execute("SELECT 1 FROM usuarios WHERE id=? AND activo=1", (user_id,)).fetchone()
    return r is not None

# Ownership checks (evita que un usuario toque cosas ajenas)
def assert_planta_owner(conn, user_id: int, planta_id: int) -> bool:
    r = conn.execute(
        "SELECT 1 FROM plantas WHERE id=? AND usuario_id=? AND activo=1",
        (planta_id, user_id)
    ).fetchone()
    return r is not None

def assert_motor_owner(conn, user_id: int, motor_id: int) -> bool:
    r = conn.execute("""
        SELECT 1
        FROM motores m
        JOIN plantas p ON p.id = m.planta_id
        WHERE m.id=? AND m.activo=1 AND p.usuario_id=? AND p.activo=1
    """, (motor_id, user_id)).fetchone()
    return r is not None

def assert_anillo_owner(conn, user_id: int, anillo_id: int) -> bool:
    r = conn.execute("""
        SELECT 1
        FROM anillos a
        JOIN motores m ON m.id = a.motor_id
        JOIN plantas p ON p.id = m.planta_id
        WHERE a.id=? AND a.activo=1 AND m.activo=1 AND p.activo=1 AND p.usuario_id=?
    """, (anillo_id, user_id)).fetchone()
    return r is not None

def assert_carbon_owner(conn, user_id: int, carbon_id: int) -> bool:
    r = conn.execute("""
        SELECT 1
        FROM carbones c
        JOIN anillos a ON a.id = c.anillo_id
        JOIN motores m ON m.id = a.motor_id
        JOIN plantas p ON p.id = m.planta_id
        WHERE c.id=? AND c.activo=1 AND a.activo=1 AND m.activo=1 AND p.activo=1 AND p.usuario_id=?
    """, (carbon_id, user_id)).fetchone()
    return r is not None

# ==========================================================
# 1) AUTH ROUTES
# ==========================================================
@app.route("/api/auth/register", methods=["POST"])
def register():
    if not require_json():
        return api_error(400, "JSON requerido")

    data = request.get_json()
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    if not email or not password:
        return api_error(400, "email y password son requeridos")

    with db_conn() as conn:
        try:
            conn.execute(
                "INSERT INTO usuarios (email, password_hash) VALUES (?,?)",
                (email, generate_password_hash(password))
            )
        except sqlite3.IntegrityError:
            return api_error(409, "Email ya existe")

    return jsonify({"msg": "Usuario creado"}), 201

@app.route("/api/auth/login", methods=["POST"])
def login():
    if not require_json():
        return api_error(400, "JSON requerido")

    data = request.get_json()
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""

    with db_conn() as conn:
        user = conn.execute(
            "SELECT id, email, password_hash, activo FROM usuarios WHERE email=?",
            (email,)
        ).fetchone()

        if not user or user["activo"] != 1:
            return api_error(401, "Credenciales inválidas")

        if not check_password_hash(user["password_hash"], password):
            return api_error(401, "Credenciales inválidas")

        token = create_access_token(identity=str(user["id"]))
        return jsonify({"token": token, "user_id": user["id"], "email": user["email"]}), 200

@app.route("/api/me", methods=["GET"])
@jwt_required()
def me():
    uid = current_user_id()
    with db_conn() as conn:
        u = conn.execute("SELECT id, email, creado_en FROM usuarios WHERE id=? AND activo=1", (uid,)).fetchone()
        if not u:
            return api_error(401, "Usuario inválido")
        return jsonify(row_to_dict(u)), 200

# ==========================================================
# 2) CRUD: PLANTAS
# ==========================================================
@app.route("/api/plantas", methods=["GET", "POST"])
@jwt_required()
def plantas():
    uid = current_user_id()

    if request.method == "GET":
        with db_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM plantas
                WHERE usuario_id=? AND activo=1
                ORDER BY id DESC
            """, (uid,)).fetchall()
            return jsonify([row_to_dict(r) for r in rows]), 200

    # POST
    if not require_json():
        return api_error(400, "JSON requerido")
    data = request.get_json()

    nombre = (data.get("nombre") or "").strip()
    if not nombre:
        return api_error(400, "nombre es requerido")

    ubicacion = (data.get("ubicacion") or "").strip() or None
    email_notif = (data.get("email_notificaciones") or "").strip().lower() or None

    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO plantas (usuario_id, nombre, ubicacion, email_notificaciones)
            VALUES (?,?,?,?)
        """, (uid, nombre, ubicacion, email_notif))
        return jsonify({"id": cur.lastrowid}), 201

@app.route("/api/plantas/<int:planta_id>", methods=["GET", "PUT", "DELETE"])
@jwt_required()
def planta_detail(planta_id: int):
    uid = current_user_id()

    with db_conn() as conn:
        if not assert_planta_owner(conn, uid, planta_id):
            return api_error(404, "Planta no existe")

        if request.method == "GET":
            p = conn.execute("SELECT * FROM plantas WHERE id=? AND activo=1", (planta_id,)).fetchone()
            return jsonify(row_to_dict(p)), 200

        if request.method == "PUT":
            if not require_json():
                return api_error(400, "JSON requerido")
            data = request.get_json()
            nombre = (data.get("nombre") or "").strip()
            ubicacion = (data.get("ubicacion") or "").strip()
            email_notif = (data.get("email_notificaciones") or "").strip().lower()

            if not nombre:
                return api_error(400, "nombre es requerido")

            conn.execute("""
                UPDATE plantas
                SET nombre=?, ubicacion=?, email_notificaciones=?
                WHERE id=? AND activo=1
            """, (nombre, ubicacion or None, email_notif or None, planta_id))
            return jsonify({"msg": "Actualizado"}), 200

        # DELETE -> soft delete + desasigna nodos + soft delete hijos
        # Se “borra” toda la jerarquía, pero NO se toca telemetría.
        eliminado = utcnow_iso()

        # 1) marcar planta inactiva
        conn.execute("UPDATE plantas SET activo=0, eliminado_en=? WHERE id=?", (eliminado, planta_id))

        # 2) buscar carbones afectados para cerrar asignaciones y liberar nodos
        carbones_ids = conn.execute("""
            SELECT c.id
            FROM carbones c
            JOIN anillos a ON a.id=c.anillo_id
            JOIN motores m ON m.id=a.motor_id
            WHERE m.planta_id=? AND c.activo=1 AND a.activo=1 AND m.activo=1
        """, (planta_id,)).fetchall()
        carbones_ids = [r["id"] for r in carbones_ids]

        # cerrar asignaciones vigentes y liberar nodos
        for cid in carbones_ids:
            asig = conn.execute("""
                SELECT id, deveui FROM asignaciones
                WHERE carbon_id=? AND fecha_fin IS NULL AND activo=1
            """, (cid,)).fetchone()
            if asig:
                conn.execute("UPDATE asignaciones SET fecha_fin=? WHERE id=?", (utcnow_iso(), asig["id"]))
                conn.execute("UPDATE nodos SET estado='disponible' WHERE deveui=?", (asig["deveui"],))

        # 3) soft delete motores/anillos/carbones (orden no crítico porque es soft)
        conn.execute("""
            UPDATE motores SET activo=0, eliminado_en=?
            WHERE planta_id=? AND activo=1
        """, (eliminado, planta_id))
        conn.execute("""
            UPDATE anillos SET activo=0, eliminado_en=?
            WHERE motor_id IN (SELECT id FROM motores WHERE planta_id=?)
              AND activo=1
        """, (eliminado, planta_id))
        conn.execute("""
            UPDATE carbones SET activo=0, eliminado_en=?
            WHERE anillo_id IN (
                SELECT a.id FROM anillos a
                JOIN motores m ON m.id=a.motor_id
                WHERE m.planta_id=?
            ) AND activo=1
        """, (eliminado, planta_id))

        return jsonify({"msg": "Planta eliminada (soft-delete). Telemetría preservada."}), 200

# ==========================================================
# 3) CRUD: MOTORES (por planta)
# ==========================================================
@app.route("/api/plantas/<int:planta_id>/motores", methods=["GET", "POST"])
@jwt_required()
def motores(planta_id: int):
    uid = current_user_id()

    with db_conn() as conn:
        if not assert_planta_owner(conn, uid, planta_id):
            return api_error(404, "Planta no existe")

        if request.method == "GET":
            rows = conn.execute("""
                SELECT * FROM motores
                WHERE planta_id=? AND activo=1
                ORDER BY id DESC
            """, (planta_id,)).fetchall()
            return jsonify([row_to_dict(r) for r in rows]), 200

        # POST
        if not require_json():
            return api_error(400, "JSON requerido")
        data = request.get_json()

        codigo = (data.get("codigo") or "").strip()
        if not codigo:
            return api_error(400, "codigo es requerido")

        payload = (
            planta_id,
            codigo,
            (data.get("modelo") or "").strip() or None,
            (data.get("ubicacion") or "").strip() or None,
            (data.get("descripcion") or "").strip() or None,
            data.get("num_anillos"),
            data.get("carbones_por_anillo"),
            data.get("alto_carbon_mm"),
            data.get("prealarma_mm"),
            data.get("minimo_cambio_mm"),
            data.get("umbral_desgaste_perc"),
            data.get("duracion_estimada_dias"),
        )

        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO motores (
                    planta_id, codigo, modelo, ubicacion, descripcion,
                    num_anillos, carbones_por_anillo, alto_carbon_mm,
                    prealarma_mm, minimo_cambio_mm, umbral_desgaste_perc,
                    duracion_estimada_dias
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, payload)
            return jsonify({"id": cur.lastrowid}), 201
        except sqlite3.IntegrityError:
            return api_error(409, "Motor duplicado (codigo ya existe en esa planta)")

@app.route("/api/motores/<int:motor_id>", methods=["GET", "PUT", "DELETE"])
@jwt_required()
def motor_detail(motor_id: int):
    uid = current_user_id()

    with db_conn() as conn:
        if not assert_motor_owner(conn, uid, motor_id):
            return api_error(404, "Motor no existe")

        if request.method == "GET":
            m = conn.execute("SELECT * FROM motores WHERE id=? AND activo=1", (motor_id,)).fetchone()
            return jsonify(row_to_dict(m)), 200

        if request.method == "PUT":
            if not require_json():
                return api_error(400, "JSON requerido")
            data = request.get_json()

            codigo = (data.get("codigo") or "").strip()
            if not codigo:
                return api_error(400, "codigo es requerido")

            try:
                conn.execute("""
                    UPDATE motores
                    SET codigo=?, modelo=?, ubicacion=?, descripcion=?,
                        num_anillos=?, carbones_por_anillo=?, alto_carbon_mm=?,
                        prealarma_mm=?, minimo_cambio_mm=?, umbral_desgaste_perc=?,
                        duracion_estimada_dias=?
                    WHERE id=? AND activo=1
                """, (
                    codigo,
                    (data.get("modelo") or "").strip() or None,
                    (data.get("ubicacion") or "").strip() or None,
                    (data.get("descripcion") or "").strip() or None,
                    data.get("num_anillos"),
                    data.get("carbones_por_anillo"),
                    data.get("alto_carbon_mm"),
                    data.get("prealarma_mm"),
                    data.get("minimo_cambio_mm"),
                    data.get("umbral_desgaste_perc"),
                    data.get("duracion_estimada_dias"),
                    motor_id
                ))
            except sqlite3.IntegrityError:
                return api_error(409, "Motor duplicado (codigo ya existe en esa planta)")

            return jsonify({"msg": "Actualizado"}), 200

        # DELETE -> soft delete motor + hijos + cerrar asignaciones
        eliminado = utcnow_iso()
        conn.execute("UPDATE motores SET activo=0, eliminado_en=? WHERE id=?", (eliminado, motor_id))

        # carbones de ese motor
        carbones_ids = conn.execute("""
            SELECT c.id
            FROM carbones c
            JOIN anillos a ON a.id=c.anillo_id
            WHERE a.motor_id=? AND c.activo=1 AND a.activo=1
        """, (motor_id,)).fetchall()
        carbones_ids = [r["id"] for r in carbones_ids]

        for cid in carbones_ids:
            asig = conn.execute("""
                SELECT id, deveui FROM asignaciones
                WHERE carbon_id=? AND fecha_fin IS NULL AND activo=1
            """, (cid,)).fetchone()
            if asig:
                conn.execute("UPDATE asignaciones SET fecha_fin=? WHERE id=?", (utcnow_iso(), asig["id"]))
                conn.execute("UPDATE nodos SET estado='disponible' WHERE deveui=?", (asig["deveui"],))

        conn.execute("UPDATE anillos SET activo=0, eliminado_en=? WHERE motor_id=? AND activo=1", (eliminado, motor_id))
        conn.execute("""
            UPDATE carbones SET activo=0, eliminado_en=?
            WHERE anillo_id IN (SELECT id FROM anillos WHERE motor_id=?)
              AND activo=1
        """, (eliminado, motor_id))

        return jsonify({"msg": "Motor eliminado (soft-delete). Telemetría preservada."}), 200

# ==========================================================
# 4) CRUD: ANILLOS (por motor)
# ==========================================================
@app.route("/api/motores/<int:motor_id>/anillos", methods=["GET", "POST"])
@jwt_required()
def anillos(motor_id: int):
    uid = current_user_id()

    with db_conn() as conn:
        if not assert_motor_owner(conn, uid, motor_id):
            return api_error(404, "Motor no existe")

        if request.method == "GET":
            rows = conn.execute("""
                SELECT * FROM anillos
                WHERE motor_id=? AND activo=1
                ORDER BY numero_anillo ASC
            """, (motor_id,)).fetchall()
            return jsonify([row_to_dict(r) for r in rows]), 200

        # POST
        if not require_json():
            return api_error(400, "JSON requerido")
        data = request.get_json()
        numero = data.get("numero_anillo")

        if numero is None:
            return api_error(400, "numero_anillo es requerido")

        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO anillos (motor_id, numero_anillo)
                VALUES (?,?)
            """, (motor_id, int(numero)))
            return jsonify({"id": cur.lastrowid}), 201
        except sqlite3.IntegrityError:
            return api_error(409, "Anillo duplicado (numero_anillo ya existe en ese motor)")

@app.route("/api/anillos/<int:anillo_id>", methods=["GET", "PUT", "DELETE"])
@jwt_required()
def anillo_detail(anillo_id: int):
    uid = current_user_id()

    with db_conn() as conn:
        if not assert_anillo_owner(conn, uid, anillo_id):
            return api_error(404, "Anillo no existe")

        if request.method == "GET":
            a = conn.execute("SELECT * FROM anillos WHERE id=? AND activo=1", (anillo_id,)).fetchone()
            return jsonify(row_to_dict(a)), 200

        if request.method == "PUT":
            if not require_json():
                return api_error(400, "JSON requerido")
            data = request.get_json()
            numero = data.get("numero_anillo")
            if numero is None:
                return api_error(400, "numero_anillo es requerido")
            try:
                conn.execute("""
                    UPDATE anillos SET numero_anillo=?
                    WHERE id=? AND activo=1
                """, (int(numero), anillo_id))
            except sqlite3.IntegrityError:
                return api_error(409, "Anillo duplicado (numero_anillo ya existe en ese motor)")
            return jsonify({"msg": "Actualizado"}), 200

        # DELETE -> soft delete anillo + carbones + liberar asignaciones/nodos
        eliminado = utcnow_iso()
        conn.execute("UPDATE anillos SET activo=0, eliminado_en=? WHERE id=?", (eliminado, anillo_id))

        carbones_ids = conn.execute("""
            SELECT id FROM carbones
            WHERE anillo_id=? AND activo=1
        """, (anillo_id,)).fetchall()
        carbones_ids = [r["id"] for r in carbones_ids]

        for cid in carbones_ids:
            asig = conn.execute("""
                SELECT id, deveui FROM asignaciones
                WHERE carbon_id=? AND fecha_fin IS NULL AND activo=1
            """, (cid,)).fetchone()
            if asig:
                conn.execute("UPDATE asignaciones SET fecha_fin=? WHERE id=?", (utcnow_iso(), asig["id"]))
                conn.execute("UPDATE nodos SET estado='disponible' WHERE deveui=?", (asig["deveui"],))

        conn.execute("UPDATE carbones SET activo=0, eliminado_en=? WHERE anillo_id=? AND activo=1", (eliminado, anillo_id))
        return jsonify({"msg": "Anillo eliminado (soft-delete). Telemetría preservada."}), 200

# ==========================================================
# 5) CRUD: CARBONES (por anillo)
# ==========================================================
@app.route("/api/anillos/<int:anillo_id>/carbones", methods=["GET", "POST"])
@jwt_required()
def carbones(anillo_id: int):
    uid = current_user_id()

    with db_conn() as conn:
        if not assert_anillo_owner(conn, uid, anillo_id):
            return api_error(404, "Anillo no existe")

        if request.method == "GET":
            rows = conn.execute("""
                SELECT c.*,
                    (
                        SELECT deveui FROM asignaciones
                        WHERE carbon_id=c.id AND fecha_fin IS NULL AND activo=1
                        ORDER BY fecha_inicio DESC LIMIT 1
                    ) AS deveui_actual
                FROM carbones c
                WHERE c.anillo_id=? AND c.activo=1
                ORDER BY c.numero_carbon ASC
            """, (anillo_id,)).fetchall()
            return jsonify([row_to_dict(r) for r in rows]), 200

        # POST
        if not require_json():
            return api_error(400, "JSON requerido")
        data = request.get_json()

        numero = data.get("numero_carbon")
        medida_inicial = data.get("medida_inicial_mm")
        umbral_alerta = data.get("umbral_alerta_perc")

        if numero is None or medida_inicial is None or umbral_alerta is None:
            return api_error(400, "numero_carbon, medida_inicial_mm, umbral_alerta_perc son requeridos")

        duracion = data.get("duracion_estimada_dias")

        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO carbones (anillo_id, numero_carbon, medida_inicial_mm, umbral_alerta_perc, duracion_estimada_dias)
                VALUES (?,?,?,?,?)
            """, (anillo_id, int(numero), float(medida_inicial), float(umbral_alerta), duracion))
            return jsonify({"id": cur.lastrowid}), 201
        except sqlite3.IntegrityError:
            return api_error(409, "Carbón duplicado (numero_carbon ya existe en ese anillo)")

@app.route("/api/carbones/<int:carbon_id>", methods=["GET", "PUT", "DELETE"])
@jwt_required()
def carbon_detail(carbon_id: int):
    uid = current_user_id()

    with db_conn() as conn:
        if not assert_carbon_owner(conn, uid, carbon_id):
            return api_error(404, "Carbón no existe")

        if request.method == "GET":
            c = conn.execute("""
                SELECT c.*,
                    (
                        SELECT deveui FROM asignaciones
                        WHERE carbon_id=c.id AND fecha_fin IS NULL AND activo=1
                        ORDER BY fecha_inicio DESC LIMIT 1
                    ) AS deveui_actual
                FROM carbones c
                WHERE c.id=? AND c.activo=1
            """, (carbon_id,)).fetchone()
            return jsonify(row_to_dict(c)), 200

        if request.method == "PUT":
            if not require_json():
                return api_error(400, "JSON requerido")
            data = request.get_json()

            medida_inicial = data.get("medida_inicial_mm")
            umbral_alerta = data.get("umbral_alerta_perc")

            if medida_inicial is None or umbral_alerta is None:
                return api_error(400, "medida_inicial_mm y umbral_alerta_perc son requeridos")

            conn.execute("""
                UPDATE carbones
                SET medida_inicial_mm=?, umbral_alerta_perc=?, duracion_estimada_dias=?
                WHERE id=? AND activo=1
            """, (float(medida_inicial), float(umbral_alerta), data.get("duracion_estimada_dias"), carbon_id))
            return jsonify({"msg": "Actualizado"}), 200

        # DELETE (soft) + cierra asignación vigente + libera nodo
        eliminado = utcnow_iso()
        asig = conn.execute("""
            SELECT id, deveui FROM asignaciones
            WHERE carbon_id=? AND fecha_fin IS NULL AND activo=1
        """, (carbon_id,)).fetchone()
        if asig:
            conn.execute("UPDATE asignaciones SET fecha_fin=? WHERE id=?", (utcnow_iso(), asig["id"]))
            conn.execute("UPDATE nodos SET estado='disponible' WHERE deveui=?", (asig["deveui"],))

        conn.execute("UPDATE carbones SET activo=0, eliminado_en=? WHERE id=?", (eliminado, carbon_id))
        return jsonify({"msg": "Carbón eliminado (soft-delete). Telemetría preservada."}), 200

# ==========================================================
# 6) CRUD: NODOS
# ==========================================================
@app.route("/api/nodos", methods=["GET", "POST"])
@jwt_required()
def nodos():
    if request.method == "GET":
        with db_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM nodos
                WHERE activo=1
                ORDER BY creado_en DESC
            """).fetchall()
            return jsonify([row_to_dict(r) for r in rows]), 200

    # POST
    if not require_json():
        return api_error(400, "JSON requerido")
    data = request.get_json()

    deveui = (data.get("deveui") or "").strip()
    if not deveui:
        return api_error(400, "deveui es requerido")

    alias = (data.get("alias") or "").strip() or None

    with db_conn() as conn:
        try:
            conn.execute("INSERT INTO nodos (deveui, alias) VALUES (?,?)", (deveui, alias))
        except sqlite3.IntegrityError:
            return api_error(409, "Nodo ya existe")
    return jsonify({"msg": "Nodo registrado"}), 201

@app.route("/api/nodos/<string:deveui>", methods=["GET", "PUT", "DELETE"])
@jwt_required()
def nodo_detail(deveui: str):
    deveui = deveui.strip()
    with db_conn() as conn:
        n = conn.execute("SELECT * FROM nodos WHERE deveui=? AND activo=1", (deveui,)).fetchone()
        if not n:
            return api_error(404, "Nodo no existe")

        if request.method == "GET":
            return jsonify(row_to_dict(n)), 200

        if request.method == "PUT":
            if not require_json():
                return api_error(400, "JSON requerido")
            data = request.get_json()
            alias = (data.get("alias") or "").strip() or None
            estado = (data.get("estado") or "").strip().lower() or None

            if estado and estado not in ("disponible", "ocupado", "inactivo"):
                return api_error(400, "estado inválido (disponible|ocupado|inactivo)")

            # si lo pones inactivo, NO se borra telemetría.
            if estado:
                conn.execute("UPDATE nodos SET alias=?, estado=? WHERE deveui=?", (alias, estado, deveui))
            else:
                conn.execute("UPDATE nodos SET alias=? WHERE deveui=?", (alias, deveui))
            return jsonify({"msg": "Actualizado"}), 200

        # DELETE -> soft delete nodo (no borra telemetría)
        # si está ocupado, primero debe desasignarse (por seguridad).
        if n["estado"] == "ocupado":
            return api_error(409, "Nodo está ocupado. Desasigna primero.")
        conn.execute("UPDATE nodos SET activo=0, estado='inactivo' WHERE deveui=?", (deveui,))
        return jsonify({"msg": "Nodo desactivado (soft-delete). Telemetría preservada."}), 200

# ==========================================================
# 7) ASIGNACIONES (enrolamiento) + historial
# ==========================================================
@app.route("/api/carbones/<int:carbon_id>/asignar", methods=["POST"])
@jwt_required()
def asignar_nodo(carbon_id: int):
    uid = current_user_id()
    if not require_json():
        return api_error(400, "JSON requerido")
    data = request.get_json()
    deveui = (data.get("deveui") or "").strip()
    if not deveui:
        return api_error(400, "deveui es requerido")

    with db_conn() as conn:
        if not assert_carbon_owner(conn, uid, carbon_id):
            return api_error(404, "Carbón no existe")

        nodo = conn.execute("SELECT * FROM nodos WHERE deveui=? AND activo=1", (deveui,)).fetchone()
        if not nodo:
            return api_error(404, "Nodo no existe")

        if nodo["estado"] == "inactivo":
            return api_error(409, "Nodo inactivo")

        # 1) cerrar asignación vigente del carbón (si existe)
        asig_actual = conn.execute("""
            SELECT id, deveui FROM asignaciones
            WHERE carbon_id=? AND fecha_fin IS NULL AND activo=1
        """, (carbon_id,)).fetchone()
        if asig_actual:
            # si es el mismo deveui, idempotente
            if asig_actual["deveui"] == deveui:
                return jsonify({"msg": "Ya estaba asignado"}), 200

            conn.execute("UPDATE asignaciones SET fecha_fin=? WHERE id=?",
                         (utcnow_iso(), asig_actual["id"]))
            conn.execute("UPDATE nodos SET estado='disponible' WHERE deveui=?",
                         (asig_actual["deveui"],))

        # 2) verificar que el nodo NO esté asignado actualmente a otro carbón
        asig_nodo = conn.execute("""
            SELECT id, carbon_id FROM asignaciones
            WHERE deveui=? AND fecha_fin IS NULL AND activo=1
        """, (deveui,)).fetchone()
        if asig_nodo:
            return api_error(409, "Nodo ya está asignado a otro carbón", {"carbon_id": asig_nodo["carbon_id"]})

        # 3) crear nueva asignación
        conn.execute("""
            INSERT INTO asignaciones (carbon_id, deveui, fecha_inicio)
            VALUES (?,?,?)
        """, (carbon_id, deveui, utcnow_iso()))
        conn.execute("UPDATE nodos SET estado='ocupado' WHERE deveui=?", (deveui,))

        return jsonify({"msg": "Asignado"}), 201

@app.route("/api/carbones/<int:carbon_id>/desasignar", methods=["POST"])
@jwt_required()
def desasignar_nodo(carbon_id: int):
    uid = current_user_id()
    with db_conn() as conn:
        if not assert_carbon_owner(conn, uid, carbon_id):
            return api_error(404, "Carbón no existe")

        asig = conn.execute("""
            SELECT id, deveui FROM asignaciones
            WHERE carbon_id=? AND fecha_fin IS NULL AND activo=1
        """, (carbon_id,)).fetchone()
        if not asig:
            return api_error(404, "Carbón no tiene nodo asignado")

        conn.execute("UPDATE asignaciones SET fecha_fin=? WHERE id=?", (utcnow_iso(), asig["id"]))
        conn.execute("UPDATE nodos SET estado='disponible' WHERE deveui=?", (asig["deveui"],))
        return jsonify({"msg": "Desasignado"}), 200

@app.route("/api/carbones/<int:carbon_id>/asignaciones", methods=["GET"])
@jwt_required()
def historial_asignaciones(carbon_id: int):
    uid = current_user_id()
    with db_conn() as conn:
        if not assert_carbon_owner(conn, uid, carbon_id):
            return api_error(404, "Carbón no existe")

        rows = conn.execute("""
            SELECT * FROM asignaciones
            WHERE carbon_id=? AND activo=1
            ORDER BY fecha_inicio DESC
        """, (carbon_id,)).fetchall()
        return jsonify([row_to_dict(r) for r in rows]), 200

# ==========================================================
# 8) TELEMETRIA (ingreso por API KEY + consulta por carbón/fechas)
# ==========================================================
@app.route("/api/telemetria", methods=["POST"])
def recibir_telemetria():
    if request.headers.get("X-API-KEY") != API_KEY_TELEMETRIA:
        return api_error(401, "Unauthorized")

    if not require_json():
        return api_error(400, "JSON requerido")

    data = request.get_json()
    deveui = (data.get("deveui") or "").strip()
    if not deveui:
        return api_error(400, "deveui es requerido")

    # Campos tolerantes: si falta alguno, queda NULL (excepto deveui)
    ax = data.get("ax"); ay = data.get("ay"); az = data.get("az")
    gx = data.get("gx"); gy = data.get("gy"); gz = data.get("gz")
    desgaste = data.get("desgaste")
    bat = data.get("bat")
    fecha = data.get("fecha")  # opcional ISO

    try:
        if fecha:
            _ = parse_iso_datetime(fecha)
            fecha_db = fecha.strip().replace("T", " ")
            if fecha_db.endswith("Z"):
                fecha_db = fecha_db[:-1]
        else:
            fecha_db = None
    except Exception:
        return api_error(400, "fecha inválida (usa ISO 8601)")

    with db_conn() as conn:
        nodo = conn.execute("SELECT deveui FROM nodos WHERE deveui=? AND activo=1", (deveui,)).fetchone()
        if not nodo:
            # Puedes decidir si auto-registrar o rechazar. Aquí: rechazar.
            return api_error(404, "Nodo no registrado")

        if fecha_db:
            conn.execute("""
                INSERT INTO telemetria (deveui, ax, ay, az, gx, gy, gz, desgaste_calculado, bateria, fecha)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (deveui, ax, ay, az, gx, gy, gz, desgaste, bat, fecha_db))
        else:
            conn.execute("""
                INSERT INTO telemetria (deveui, ax, ay, az, gx, gy, gz, desgaste_calculado, bateria)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (deveui, ax, ay, az, gx, gy, gz, desgaste, bat))

        if bat is not None:
            conn.execute("UPDATE nodos SET bateria=? WHERE deveui=?", (int(bat), deveui))

    return jsonify({"status": "stored"}), 201

def _telemetry_query_for_carbon(conn, carbon_id: int, from_dt: dt.datetime, to_dt: dt.datetime, limit: int, offset: int):
    """
    Retorna telemetría asociada al carbón en el rango, usando historial de asignaciones:
    telemetria.deveui coincide con asignaciones.deveui
    y telemetria.fecha está entre fecha_inicio y fecha_fin (o fecha_fin NULL).
    """
    from_s = from_dt.strftime("%Y-%m-%d %H:%M:%S")
    to_s = to_dt.strftime("%Y-%m-%d %H:%M:%S")

    rows = conn.execute(f"""
        SELECT t.*
        FROM telemetria t
        JOIN asignaciones a ON a.deveui = t.deveui
        WHERE a.carbon_id = ?
          AND a.activo=1
          AND t.fecha BETWEEN ? AND ?
          AND t.fecha >= replace(a.fecha_inicio,'T',' ')
          AND (a.fecha_fin IS NULL OR t.fecha <= replace(a.fecha_fin,'T',' '))
        ORDER BY t.fecha ASC
        LIMIT ? OFFSET ?
    """, (carbon_id, from_s, to_s, limit, offset)).fetchall()

    return [row_to_dict(r) for r in rows]

@app.route("/api/carbones/<int:carbon_id>/telemetria", methods=["GET"])
@jwt_required()
def telemetria_por_carbon(carbon_id: int):
    uid = current_user_id()

    # Query params
    q_from = request.args.get("from")
    q_to = request.args.get("to")
    limit = int(request.args.get("limit", "500"))
    offset = int(request.args.get("offset", "0"))

    if limit < 1 or limit > 5000:
        return api_error(400, "limit inválido (1..5000)")
    if offset < 0:
        return api_error(400, "offset inválido")

    # defaults: últimas 24h si no viene nada
    now = dt.datetime.utcnow().replace(microsecond=0)
    try:
        if q_to:
            to_dt = parse_iso_datetime(q_to)
        else:
            to_dt = now
        if q_from:
            from_dt = parse_iso_datetime(q_from)
        else:
            from_dt = to_dt - dt.timedelta(hours=24)
    except Exception:
        return api_error(400, "from/to inválidos (usa ISO 8601)")

    if from_dt > to_dt:
        return api_error(400, "from debe ser <= to")

    with db_conn() as conn:
        if not assert_carbon_owner(conn, uid, carbon_id):
            return api_error(404, "Carbón no existe")

        data = _telemetry_query_for_carbon(conn, carbon_id, from_dt, to_dt, limit, offset)

        # Alarmas simples: usa el último punto del rango (si existe)
        alarm = None
        if data:
            last = data[-1]
            desgaste = last.get("desgaste_calculado")
            carbon = conn.execute("SELECT umbral_alerta_perc FROM carbones WHERE id=?", (carbon_id,)).fetchone()
            if carbon and desgaste is not None:
                alarm = {"umbral_alerta_perc": carbon["umbral_alerta_perc"], "desgaste_calculado": desgaste,
                         "en_alerta": float(desgaste) >= float(carbon["umbral_alerta_perc"])}

        return jsonify({
            "carbon_id": carbon_id,
            "from": q_from or (from_dt.isoformat() + "Z"),
            "to": q_to or (to_dt.isoformat() + "Z"),
            "count": len(data),
            "offset": offset,
            "limit": limit,
            "alarm": alarm,
            "data": data
        }), 200

@app.route("/api/carbones/<int:carbon_id>/exportar", methods=["GET"])
@jwt_required()
def exportar_csv_carbon(carbon_id: int):
    uid = current_user_id()
    q_from = request.args.get("from")
    q_to = request.args.get("to")

    now = dt.datetime.utcnow().replace(microsecond=0)
    try:
        to_dt = parse_iso_datetime(q_to) if q_to else now
        from_dt = parse_iso_datetime(q_from) if q_from else (to_dt - dt.timedelta(days=7))
    except Exception:
        return api_error(400, "from/to inválidos (usa ISO 8601)")

    if from_dt > to_dt:
        return api_error(400, "from debe ser <= to")

    with db_conn() as conn:
        if not assert_carbon_owner(conn, uid, carbon_id):
            return api_error(404, "Carbón no existe")

        data = _telemetry_query_for_carbon(conn, carbon_id, from_dt, to_dt, limit=500000, offset=0)

        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerow(["id", "deveui", "ax", "ay", "az", "gx", "gy", "gz", "desgaste_calculado", "bateria", "fecha"])
        for r in data:
            cw.writerow([
                r.get("id"), r.get("deveui"),
                r.get("ax"), r.get("ay"), r.get("az"),
                r.get("gx"), r.get("gy"), r.get("gz"),
                r.get("desgaste_calculado"), r.get("bateria"),
                r.get("fecha")
            ])

        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = f"attachment; filename=telemetria_carbon_{carbon_id}.csv"
        output.headers["Content-type"] = "text/csv"
        return output, 200

# ==========================================================
# MAIN
# ==========================================================
# if __name__ == "__main__":
#     init_db()
#     # En producción: usa gunicorn/uwsgi, no app.run
#     app.run(host="0.0.0.0", port=5000, debug=False)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)