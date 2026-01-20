from flask import Flask, render_template, jsonify
from pymongo import MongoClient

app = Flask(__name__)

client = MongoClient("mongodb://mongodb:27017/")
db = client["steam_project"]
collection = db["games_resultado"]


@app.route("/")
def page_games():
    return render_template(
        "index.html", title="Juegos Principales", api_endpoint="/api/games"
    )


@app.route("/dlcs")
def page_dlcs():
    return render_template(
        "index.html", title="DLCs y Expansiones", api_endpoint="/api/dlcs"
    )


@app.route("/business")
def page_business():
    return render_template("business.html")


@app.route("/api/business-data")
def api_business_data():
    raw_data = list(db["insights_business"].find())

    response = {}

    for doc in raw_data:
        title = doc.get("titulo")
        rows = doc.get("data", [])

        if title == "Engagement por Tipo":
            response["engagement"] = {
                "labels": [r["Tipo"] for r in rows],
                "values": [
                    (
                        r["Satisfaccion_Promedio"]
                        if "Satisfaccion_Promedio" in r
                        else r.get("Calidad")
                    )
                    for r in rows
                ],
                "counts": [
                    (
                        r["Engagement_Total"]
                        if "Engagement_Total" in r
                        else r.get("Engagement")
                    )
                    for r in rows
                ],
            }

        elif title == "Impacto Masividad":
            order = {"Nicho": 1, "Medio": 2, "Viral": 3}
            rows.sort(key=lambda x: order.get(x["Trafico"], 99))

            response["traffic"] = {
                "labels": [r["Trafico"] for r in rows],
                "values": [r["Calidad"] or r.get("Calidad_Promedio") for r in rows],
            }

        elif title == "Efectividad Demos":
            response["early"] = {
                "labels": [r["Version"] for r in rows],
                "values": [r["Calidad"] or r.get("Calidad_Promedio") for r in rows],
            }

    return jsonify(response)


def get_data_by_type(target_type):
    data = list(collection.find())
    processed = []
    total_pos = 0
    total_neg = 0

    for item in data:
        raw_name = item.get("game", "Unknown")

        if "::" in raw_name:
            parts = raw_name.split("::", 1)
            category = parts[0]
            clean_name = parts[1]
        else:
            category = "GAME"
            clean_name = raw_name

        pos = int(item.get("positivos", 0))
        neg = int(item.get("negativos", 0))

        if category == target_type:
            total_pos += pos
            total_neg += neg

            total = pos + neg
            min_votes = 100 if target_type == "GAME" else 20

            if total > min_votes:
                ratio = (pos / total) * 100
                processed.append({"game": clean_name, "ratio": ratio, "total": total})

    processed.sort(key=lambda x: x["ratio"], reverse=True)

    return {
        "global_pos": total_pos,
        "global_neg": total_neg,
        "top": processed[:5],
        "bottom": processed[-5:],
    }


@app.route("/api/games")
def api_games():
    return jsonify(get_data_by_type("GAME"))


@app.route("/api/dlcs")
def api_dlcs():
    return jsonify(get_data_by_type("DLC"))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
