from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from db import SessionLocal
from models import User
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

@app.route('/api/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return jsonify({'error': 'Missing fields'}), 400

    hashed = generate_password_hash(password)
    with SessionLocal() as s:
        if s.query(User).filter_by(username=username).first():
            return jsonify({'error': 'User already exists'}), 400
        user = User(username=username, password=hashed)
        s.add(user)
        s.commit()
    return jsonify({'message': 'Registered successfully'}), 201


@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    with SessionLocal() as s:
        user = s.query(User).filter_by(username=username).first()
        if not user or not check_password_hash(user.password, password):
            return jsonify({'error': 'Invalid credentials'}), 401
    return jsonify({'message': 'Login successful'}), 200
