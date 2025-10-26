from flask import Flask, request, jsonify, g
from flask_cors import CORS
import yfinance as yf
import requests 
import os # WICHTIG: Für Umgebungsvariablen
import psycopg2 # WICHTIG: Der PostgreSQL-Treiber
from psycopg2.extras import RealDictCursor # WICHTIG: Um Dicts statt Tupeln zu bekommen

# --- Konfiguration ---
app = Flask(__name__)
CORS(app) 

# Hole die Datenbank-URL aus den Umgebungsvariablen
DATABASE_URL = os.environ.get('DATABASE_URL')
# NEU: Geheimer Schlüssel für den Cron Job (in Render-Umgebungsvariablen festlegen)
CRON_SECRET = os.environ.get('CRON_SECRET')

# --- 0. Hilfsfunktionen (Kurs holen & DB-Verbindung) ---

def get_db():
    """
    Stellt eine Verbindung zur Supabase-Datenbank her.
    """
    if 'db' not in g:
        if DATABASE_URL is None:
            raise ValueError("DATABASE_URL ist nicht in den Umgebungsvariablen gesetzt!")
        
        g.db = psycopg2.connect(DATABASE_URL)
    return g.db

@app.teardown_appcontext
def close_connection(exception):
    """
    Schließt die Datenbankverbindung.
    """
    db = g.pop('db', None)
    if db is not None:
        db.close()

def get_ticker_info(ticker_symbol):
    """
    Holt aktuelle Kursdaten, prozentuale Veränderung und Namen für einen Ticker.
    Gibt ein Dictionary zurück oder None bei Fehler.
    """
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info
        
        current_price = info.get('currentPrice', info.get('regularMarketPrice'))
        day_change_pct = info.get('regularMarketChangePercent', info.get('marketChangePercent', 0))
        name = info.get('longName', info.get('shortName', ticker_symbol)) 

        if current_price is None:
            hist = ticker.history(period="1d")
            if not hist.empty:
                current_price = hist['Close'].iloc[-1]
            else:
                 return None 

        if day_change_pct == 0:
             hist_2d = ticker.history(period="2d")
             if len(hist_2d) >= 2:
                 prev_close = hist_2d['Close'].iloc[-2]
                 if prev_close != 0:
                     day_change_pct = (current_price - prev_close) / prev_close

        return {
            "price": current_price,
            "change_pct": day_change_pct,
            "name": name
        }

    except Exception as e:
        print(f"Fehler beim Abrufen der Ticker-Info für {ticker_symbol}: {e}")
        return None

# --- 1. Die API-Endpunkte (DEIN BESTEHENDER CODE) ---

@app.route("/")
def index():
    return "Willkommen beim TestBroker API!"

@app.route("/buy", methods=['POST'])
def buy_stock():
    data = request.get_json()
    user_id = data['user_id']
    ticker = data['ticker'].strip().upper() 
    quantity = float(data['quantity'])
    if quantity <= 0: return jsonify({"error": "Anzahl > 0"}), 400
    
    ticker_data = get_ticker_info(ticker) 
    if ticker_data is None:
        return jsonify({"error": f"Kurs für Ticker {ticker} nicht gefunden."}), 404
    price = ticker_data['price']

    total_cost = price * quantity
    db = get_db()
    
    with db.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT cash_balance FROM accounts WHERE user_id = %s", (user_id,))
        account = cursor.fetchone()
        
        if account is None: return jsonify({"error": f"Nutzer {user_id} nicht gefunden."}), 404
        if account['cash_balance'] < total_cost: return jsonify({"error": "Nicht genügend Bargeld."}), 400
        
        new_cash_balance = account['cash_balance'] - total_cost
        cursor.execute("UPDATE accounts SET cash_balance = %s WHERE user_id = %s", (new_cash_balance, user_id))
        
        cursor.execute("SELECT * FROM positions WHERE user_id = %s AND ticker_symbol = %s", (user_id, ticker))
        position = cursor.fetchone()
        
        if position:
            old_quantity = position['quantity']
            old_avg_price = position['average_buy_price']
            new_quantity = old_quantity + quantity
            new_avg_price = ((old_avg_price * old_quantity) + (price * quantity)) / new_quantity
            cursor.execute("UPDATE positions SET quantity = %s, average_buy_price = %s WHERE position_id = %s", 
                           (new_quantity, new_avg_price, position['position_id']))
        else:
            cursor.execute("INSERT INTO positions (user_id, ticker_symbol, quantity, average_buy_price) VALUES (%s, %s, %s, %s)", 
                           (user_id, ticker, quantity, price))
        
        cursor.execute("INSERT INTO transactions (user_id, ticker_symbol, transaction_type, quantity, price_per_share) VALUES (%s, %s, 'BUY', %s, %s)", 
                       (user_id, ticker, quantity, price))
    
    db.commit() # Speichern
    return jsonify({"message": "Kauf erfolgreich!"}), 201


@app.route("/sell", methods=['POST'])
def sell_stock():
    data = request.get_json()
    user_id = data['user_id']
    ticker = data['ticker'].strip().upper()
    quantity_to_sell = float(data['quantity'])
    if quantity_to_sell <= 0: return jsonify({"error": "Anzahl > 0"}), 400
    
    db = get_db()
    with db.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT * FROM positions WHERE user_id = %s AND ticker_symbol = %s", (user_id, ticker))
        position = cursor.fetchone()
        
        if position is None or position['quantity'] < quantity_to_sell: 
            return jsonify({"error": "Nicht genügend Stücke."}), 400
        
        ticker_data = get_ticker_info(ticker) 
        if ticker_data is None:
            return jsonify({"error": f"Kurs für Ticker {ticker} nicht gefunden."}), 404
        price = ticker_data['price']

        total_revenue = price * quantity_to_sell
        
        cursor.execute("SELECT cash_balance FROM accounts WHERE user_id = %s", (user_id,))
        account = cursor.fetchone()
        
        new_cash_balance = account['cash_balance'] + total_revenue
        cursor.execute("UPDATE accounts SET cash_balance = %s WHERE user_id = %s", (new_cash_balance, user_id))
        
        new_quantity = position['quantity'] - quantity_to_sell
        if new_quantity <= 0.0000001: # Toleranz für Fließkommazahlen
            cursor.execute("DELETE FROM positions WHERE position_id = %s", (position['position_id'],))
        else:
            cursor.execute("UPDATE positions SET quantity = %s WHERE position_id = %s", (new_quantity, position['position_id']))
        
        cursor.execute("INSERT INTO transactions (user_id, ticker_symbol, transaction_type, quantity, price_per_share) VALUES (%s, %s, 'SELL', %s, %s)", 
                       (user_id, ticker, quantity_to_sell, price))
    
    db.commit() # Speichern
    return jsonify({"message": "Verkauf erfolgreich!"}), 200

@app.route("/portfolio/<user_id>", methods=['GET'])
def get_portfolio(user_id):
    db = get_db()
    with db.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT cash_balance FROM accounts WHERE user_id = %s", (user_id,))
        account = cursor.fetchone()
        if account is None: return jsonify({"error": f"Nutzer {user_id} nicht gefunden."}), 404
            
        cash_balance = account['cash_balance']
        
        cursor.execute("SELECT ticker_symbol, quantity, average_buy_price FROM positions WHERE user_id = %s", (user_id,))
        positions = cursor.fetchall()
    
    total_value_stocks = 0
    total_value_crypto = 0
    total_investment_cost = 0 
    detailed_positions = []

    for pos in positions:
        ticker = pos['ticker_symbol']
        quantity = pos['quantity']
        avg_buy_price = pos['average_buy_price']
        
        position_investment_cost = avg_buy_price * quantity 
        total_investment_cost += position_investment_cost  
        
        ticker_data = get_ticker_info(ticker) 
        current_price = 0
        day_change_pct = 0 
        name = ticker 

        if ticker_data:
             current_price = ticker_data['price']
             day_change_pct = ticker_data['change_pct'] 
             name = ticker_data['name'] 
        
        position_value = current_price * quantity
        unrealized_pnl = (current_price - avg_buy_price) * quantity
        
        unrealized_pnl_pct = 0
        if position_investment_cost > 0: 
            unrealized_pnl_pct = unrealized_pnl / position_investment_cost 
        
        if ticker.endswith('-USD'):
            total_value_crypto += position_value
        else:
            total_value_stocks += position_value
            
        detailed_positions.append({
            "ticker": ticker,
            "name": name,
            "quantity": quantity,
            "average_buy_price": avg_buy_price,
            "current_price": current_price,
            "current_value": position_value,
            "unrealized_pnl": unrealized_pnl,
            "unrealized_pnl_pct": unrealized_pnl_pct,
            "day_change_pct": day_change_pct 
        })

    total_asset_value = total_value_stocks + total_value_crypto
    total_portfolio_value = total_asset_value + cash_balance
    
    overall_pnl_pct = 0
    if total_investment_cost > 0:
        overall_pnl = total_asset_value - total_investment_cost
        overall_pnl_pct = overall_pnl / total_investment_cost

    return jsonify({
        "user_id": user_id,
        "cash_balance": cash_balance,
        "total_asset_value": total_asset_value,
        "total_value_stocks": total_value_stocks,
        "total_value_crypto": total_value_crypto,
        "total_portfolio_value": total_portfolio_value,
        "overall_pnl_pct": overall_pnl_pct, 
        "positions": detailed_positions
    }), 200

@app.route("/search/<query>")
def search_ticker(query):
    url = f"https://query1.finance.yahoo.com/v1/finance/search?q={query}&lang=en-US&region=US&quotesCount=8&newsCount=0"
    headers = {'User-Agent': 'Mozilla/5.0'} 
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        results = []
        if 'quotes' in data:
            for quote in data['quotes']:
                name = quote.get('longname', quote.get('shortname'))
                if not name: continue 

                if quote.get('quoteType') == 'EQUITY':
                    results.append({"symbol": quote['symbol'], "name": name})
                elif quote.get('quoteType') == 'CRYPTOCURRENCY':
                     results.append({"symbol": quote['symbol'], "name": name})
        return jsonify(results)
    except Exception as e:
        print(f"Fehler bei der Ticker-Suche: {e}")
        return jsonify({"error": "Suche fehlgeschlagen"}), 500

@app.route("/market_data")
def get_market_data():
    market_tickers = {
        "DAX": "^GDAXI",
        "Nasdaq": "^IXIC",
        "Dow Jones": "^DJI",
        "Nikkei": "^N225",
        "S&P 500": "^GSPC",
        "Gold": "GC=F",
        "Bitcoin": "BTC-USD"
    }
    
    results = {}
    for name, ticker in market_tickers.items():
        data = get_ticker_info(ticker)
        if data:
            results[name] = {
                "price": data['price'],
                "change_pct": data['change_pct']
            }
        else:
            results[name] = {"price": "N/A", "change_pct": 0} 
            
    return jsonify(results)

# ===================================================================
# ===== NEUER BEREICH: Historien-Endpunkte =====
# ===================================================================

@app.route("/api/record-history", methods=['POST'])
def record_portfolio_history():
    """
    NEUER ENDPUNKT (Schritt 2)
    Wird vom Render Cron Job aufgerufen, um den aktuellen Wert
    aller Portfolios in die Supabase-Tabelle 'portfolio_history' zu schreiben.
    """
    # 1. Sicherer Endpunkt, den nur der Cron Job aufrufen kann
    auth_header = request.headers.get('Authorization')
    if not CRON_SECRET or auth_header != f"Bearer {CRON_SECRET}":
        print("Cron Job Fehler: Nicht autorisierter Zugriff.")
        return jsonify({"error": "Nicht autorisiert"}), 401
    
    print("Cron Job: Starte Aufzeichnung des Portfolio-Verlaufs...")
    db = get_db()
    with db.cursor(cursor_factory=RealDictCursor) as cursor:
        
        # 2. Hole alle User-IDs aus der 'accounts'-Tabelle
        cursor.execute("SELECT DISTINCT user_id FROM accounts")
        users = cursor.fetchall()
        
        count = 0
        for user in users:
            user_id = user['user_id']
            
            try:
                # 3. Berechne den Gesamtwert für jeden User
                # (Vereinfachte Logik von /portfolio/<user_id>, da wir nicht alle Details brauchen)
                
                cursor.execute("SELECT cash_balance FROM accounts WHERE user_id = %s", (user_id,))
                account = cursor.fetchone()
                cash_balance = account['cash_balance'] if account else 0
                
                cursor.execute("SELECT ticker_symbol, quantity FROM positions WHERE user_id = %s", (user_id,))
                positions = cursor.fetchall()
                
                total_asset_value = 0
                for pos in positions:
                    # Nutze die bestehende Funktion, um den aktuellen Preis zu holen
                    ticker_data = get_ticker_info(pos['ticker_symbol'])
                    if ticker_data:
                        total_asset_value += ticker_data['price'] * pos['quantity']
                        
                total_portfolio_value = cash_balance + total_asset_value
                
                # 4. Speichere den Wert in der neuen Tabelle
                cursor.execute(
                    "INSERT INTO portfolio_history (user_id, value) VALUES (%s, %s)",
                    (user_id, total_portfolio_value)
                )
                count += 1
                
            except Exception as e:
                print(f"Fehler bei Aufzeichnung für User {user_id}: {e}")
                # Nicht abbrechen, mit nächstem User weitermachen
        
        db.commit() # Speichere alle Einträge
    
    print(f"Cron Job: {count} Portfolio-Werte erfolgreich gespeichert.")
    return jsonify({"message": f"{count} Portfolio-Werte erfolgreich gespeichert."}), 200


@app.route("/portfolio/<user_id>/history", methods=['GET'])
def get_portfolio_history(user_id):
    """
    NEUER ENDPUNKT (Schritt 3)
    Dieser Endpunkt wird vom Frontend (Netlify) aufgerufen.
    Er liest die gespeicherten Daten aus der 'portfolio_history' Tabelle.
    """
    range = request.args.get('range', '1d').lower() # z.B. ?range=1w
    
    db = get_db()
    with db.cursor(cursor_factory=RealDictCursor) as cursor:
        
        # 1. Wähle das richtige Zeitintervall
        if range == '1w':
            interval = '7 days'
            # Für 1W: Nimm alle stündlichen Datenpunkte
            sql = """
                SELECT created_at as timestamp, value
                FROM portfolio_history
                WHERE user_id = %s AND created_at >= NOW() - INTERVAL %s
                ORDER BY created_at ASC;
            """
            params = (user_id, interval)
            
        elif range == '1m':
            interval = '1 month'
            # Für 1M: Nimm auch alle stündlichen Datenpunkte
            sql = """
                SELECT created_at as timestamp, value
                FROM portfolio_history
                WHERE user_id = %s AND created_at >= NOW() - INTERVAL %s
                ORDER BY created_at ASC;
            """
            params = (user_id, interval)

        elif range == '1y':
            interval = '1 year'
            # WICHTIG: Für 1Y die Daten bündeln (z.B. 1 Wert pro Tag)
            # Sonst sendest du >8000 Datenpunkte
            sql = """
                SELECT 
                  date_trunc('day', created_at) as timestamp, -- Bündelt auf den Tag
                  AVG(value) as value -- Nimmt den Durchschnittswert des Tages
                FROM portfolio_history
                WHERE user_id = %s AND created_at >= NOW() - INTERVAL %s
                GROUP BY 1 -- Gruppiert nach dem "timestamp" (dem Tag)
                ORDER BY 1 ASC; -- Sortiert nach dem Tag
            """
            params = (user_id, interval)
            
        else: # Fallback für "1d" (den das Frontend nicht nutzt, aber sicher ist sicher)
            interval = '1 day'
            sql = """
                SELECT created_at as timestamp, value
                FROM portfolio_history
                WHERE user_id = %s AND created_at >= NOW() - INTERVAL %s
                ORDER BY created_at ASC;
            """
            params = (user_id, interval)

        # 2. Führe die Abfrage aus
        cursor.execute(sql, params)
        history_data = cursor.fetchall()
        
        # 3. Sende die Daten als JSON
        # Das Format (Liste von Dicts mit "timestamp" and "value")
        # passt genau zu dem, was das Frontend erwartet.
        return jsonify(history_data), 200


# --- Den Server starten ---
if __name__ == "__main__":
    # WICHTIG: Port wird von Render.com vorgegeben
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
