from flask import Flask, request, jsonify
import sys
import os
import json
import random
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__)

@app.route('/ghost', methods=['GET'])
def ghost_endpoint():
    """API لإرسال الأشباح"""
    team_code = request.args.get('team_code')
    name = request.args.get('name', 'OTMAN')
    
    if not team_code:
        return jsonify({
            "success": False,
            "error": "team_code parameter is required"
        }), 400
    
    # محاكاة إرسال 3 أشباح
    results = []
    success_count = 0
    
    for i in range(3):
        try:
            # هنا يمكنك إضافة منطق إرسال الشبح الحقيقي
            results.append({
                "index": i + 1,
                "name": name,
                "status": "SENT",
                "team_code": team_code
            })
            success_count += 1
            time.sleep(0.1)
        except Exception as e:
            results.append({
                "index": i + 1,
                "name": name,
                "status": "FAILED",
                "error": str(e)
            })
    
    return jsonify({
        "status": "success" if success_count > 0 else "failed",
        "team_code": team_code,
        "ghost_name": name,
        "total_ghosts": 3,
        "success_count": success_count,
        "failed_count": 3 - success_count,
        "results": results
    })

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "status": "online",
        "message": "Ghost API is running",
        "endpoints": {
            "/ghost": "GET - Send ghosts (params: team_code, name)"
        }
    })

# لـ Vercel Serverless
app = app

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)