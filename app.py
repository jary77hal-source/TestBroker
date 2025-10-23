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

# NEU: Hole die Datenbank-URL aus den Umgebungsvariablen
DATABASE_URL = os.environ.get('DATABASE_URL')

# --- 0. Hilfsfunktionen (Kurs holen & DB-Verbindung) ---

def get_db():
    """
    Stellt eine Verbindung zur Supabase-Datenbank her.
    """
    if 'db' not in g:
        if DATABASE_URL is None:
            raise ValueError("DATABASE_URL ist nicht in den Umgebungsvariablen gesetzt!")
        
        # NEU: Verbindung mit psycopg2
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

# --- 1. Die API-Endpunkte ---

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
    
    # WICHTIG: %s statt ? als Platzhalter für PostgreSQL!
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
    # WICHTIG: %s statt ? als Platzhalter
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
    # WICHTIG: %s statt ? als Platzhalter
    with db.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute("SELECT cash_balance FROM accounts WHERE user_id = %s", (user_id,))
        account = cursor.fetchone()
        if account is None: return jsonify({"error": f"Nutzer {user_id} nicht gefunden."}), 404
            
        cash_balance = account['cash_balance']
        
        cursor.execute("SELECT ticker_symbol, quantity, average_buy_price FROM positions WHERE user_id = %s", (user_id,))
        positions = cursor.fetchall()
    
    # Der Rest der Logik (außerhalb der DB-Abfrage) bleibt gleich
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
    # Dieser Teil braucht keine DB und bleibt gleich
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
    # Dieser Teil braucht keine DB und bleibt gleich
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

# --- Den Server starten ---
if __name__ == "__main__":
    # WICHTIG: Port wird von Render.com vorgegeben
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=False, host='0.0.0.0', port=port)
