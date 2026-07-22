import asyncio
import websockets
import json
import os
import sympy
import shlex
import hashlib
import time
import pymongo
from sympy.parsing.sympy_parser import parse_expr, standard_transformations, implicit_multiplication_application

connected_clients = {}

SESSION_TOTAL_TIME = 60 * 60
time_left = SESSION_TOTAL_TIME
session_running = False
tasks_done_this_session = set() 

STATE_FILE = "server_state.json"
MAX_HISTORY = 60

board_state = {
    "task_counter": 0,
    "tasks": []
}
chat_history = []
user_profiles = {} 
dead_drops = {}
active_poll = None
last_chat_purge = time.time()

# --- PALETTE DE COULEURS UNIQUES POUR LES UTILISATEURS ---
USER_UNIQUE_PALETTE = [
    "#00ff41", "#00e5ff", "#b500ff", "#ff7700", 
    "#ff00aa", "#0099ff", "#33ff88", "#e6ff00",
    "#ff3366", "#00ffaa", "#ffcc00", "#9966ff"
]

def get_user_default_color(username: str) -> str:
    """Génère une couleur par défaut unique et déterministe basée sur le pseudo."""
    hash_val = int(hashlib.md5(username.lower().encode()).hexdigest(), 16)
    return USER_UNIQUE_PALETTE[hash_val % len(USER_UNIQUE_PALETTE)]

def get_rank_and_unlocks(level: int, default_color: str):
    """Retourne le rang et la liste des couleurs débloquées."""
    # Le niveau 1 (Novice) n'a QUE sa couleur par défaut unique
    unlocks = {
        "defaut": default_color,
        "default": default_color
    }
    
    if level >= 2:
        unlocks["cyan"] = "#00e5ff"
        unlocks["vert"] = "#00ff41"
    if level >= 4:
        unlocks["bleu"] = "#0088ff"
        unlocks["violet"] = "#b500ff"
    if level >= 7:
        unlocks["rose"] = "#ff00aa"
        unlocks["menthe"] = "#33ff88"
    if level >= 10:
        unlocks["or"] = "#ffd700"
        
    rank = "Novice"
    if level >= 10: rank = "Expert"
    elif level >= 7: rank = "Confirmé"
    elif level >= 4: rank = "Spécialiste"
    elif level >= 2: rank = "Initié"
    
    return rank, unlocks

def add_exp(username, amount):
    profile = user_profiles.get(username)
    if not profile: return False
    
    profile["exp"] += amount
    new_level = (profile["exp"] // 100) + 1
    
    leveled_up = False
    if new_level > profile["level"]:
        profile["level"] = new_level
        leveled_up = True
        
    profile["rank"], _ = get_rank_and_unlocks(profile["level"], profile["default_color"])
    save_state()
    return leveled_up

def check_board_combo():
    if not board_state["tasks"]: return False
    return all(task["done"] for task in board_state["tasks"])

# --- CONNEXION À LA BASE DE DONNÉES CLOUD ---
# Au lieu de l'URL en clair, on demande à Render de fournir la clé secrète
MONGO_URI = os.environ.get("MONGO_URI")

# Si on teste en local et que la variable n'existe pas, on prévoit une sécurité
if not MONGO_URI:
    print("ATTENTION : Clé MongoDB introuvable.")
try:
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client["nexus_database"]
    collection = db["server_state"]
except Exception as e:
    print("CRITIQUE : Impossible de joindre MongoDB", e)

def save_state():
    """Sauvegarde le JSON en écrasant l'ancienne version dans le cloud."""
    try:
        state_data = {
            "_id": "master_state", # Cet ID unique garantit qu'on modifie toujours le même fichier
            "board_state": board_state,
            "chat_history": chat_history,
            "user_profiles": user_profiles,
            "dead_drops": dead_drops,
            "last_chat_purge": last_chat_purge
        }
        # replace_one va remplacer le document existant, ou le créer s'il n'y en a pas
        collection.replace_one({"_id": "master_state"}, state_data, upsert=True)
    except Exception as e:
        print(f"Erreur de sauvegarde cloud : {e}")

def load_state():
    """Récupère le JSON depuis le cloud au démarrage du serveur."""
    global board_state, chat_history, user_profiles, dead_drops, last_chat_purge
    
    try:
        # On fouille la base de données pour trouver notre sauvegarde
        data = collection.find_one({"_id": "master_state"})
        
        if data:
            board_state = data.get("board_state", board_state)
            chat_history = data.get("chat_history", [])
            user_profiles = data.get("user_profiles", {})
            dead_drops = data.get("dead_drops", {})
            last_chat_purge = data.get("last_chat_purge", time.time())
            print(">>> ARCHIVES CLOUD RÉCUPÉRÉES AVEC SUCCÈS <<<")
        else:
            print(">>> AUCUNE ARCHIVE TROUVÉE. INITIALISATION À ZÉRO. <<<")
            
    except Exception as e:
        print(f"Erreur de chargement cloud : {e}")

def append_to_history(msg_dict):
    chat_history.append(msg_dict)
    if len(chat_history) > MAX_HISTORY:
        chat_history.pop(0)
    save_state()

async def broadcast_system_msg(text, color="#ffaa00"):
    """Les messages SYSTÈME utilisent par défaut l'ambre réservé #ffaa00."""
    sys_dict = {"type": "chat", "username": "SYSTÈME", "color": color, "value": text}
    append_to_history(sys_dict)
    payload = json.dumps(sys_dict)
    for client in list(connected_clients.keys()):
        try: await client.send(payload)
        except: pass

def generate_poll_text(poll_data):
    total_votes = len(poll_data["votes"])
    lines = [f"[bold orange]► {poll_data['question']}[/]" ]
    for i, opt in enumerate(poll_data["options"]):
        count = sum(1 for v in poll_data["votes"].values() if v == i)
        pct = (count / total_votes * 100) if total_votes > 0 else 0
        bar_len = int(pct / 10)
        bar = "█" * bar_len + "░" * (10 - bar_len)
        lines.append(f"[bold #ffaa00]/vote {i+1}[/] - {opt.ljust(15)} | [bold #00e5ff]{bar}[/] {pct:.0f}% ({count})")
    return "\n".join(lines)

async def global_timer():
    global time_left, session_running, tasks_done_this_session, last_chat_purge
    
    # 7 jours = 604800 secondes 
    PURGE_INTERVAL = 604800 
    
    while True:
        await asyncio.sleep(1)
        
        # --- NOUVEAU : VÉRIFICATION DE LA PURGE ---
        current_time = time.time()
        if current_time - last_chat_purge >= PURGE_INTERVAL:
            chat_history.clear()
            last_chat_purge = current_time
            save_state()
            
            purge_payload = json.dumps({"type": "chat_clear"})
            for client in list(connected_clients.keys()):
                try: await client.send(purge_payload)
                except: pass
                
        if session_running and time_left > 0:
            time_left -= 1
            minutes, seconds = divmod(time_left, 60)
            payload = json.dumps({"type": "timer", "value": f"{minutes:02d}:{seconds:02d}"})
            for client in list(connected_clients.keys()):
                try: await client.send(payload)
                except: pass
                
            if time_left == 0:
                session_running = False
                await broadcast_system_msg("SESSION DE FOCUS TERMINÉE. +50 EXP POUR TOUS LES AGENTS !", "#ffaa00")
                for ws, uname in connected_clients.items():
                    if add_exp(uname, 50):
                        await broadcast_system_msg(f"PROMOTION : {uname} a atteint le Niveau {user_profiles[uname]['level']} ({user_profiles[uname]['rank']}) !", "#ffaa00")
                
                stop_payload = json.dumps({"type": "session_stopped"})
                for client in list(connected_clients.keys()):
                    try: await client.send(stop_payload)
                    except: pass

async def mission_control_hub(websocket):
    global board_state, session_running, time_left, tasks_done_this_session, active_poll, dead_drops
    try:
        auth_message = await websocket.recv()
        data = json.loads(auth_message)
        
        if data.get("type") == "login":
            username = data.get("username")
            if username in connected_clients.values():
                await websocket.send(json.dumps({"type": "login_error", "message": "Pseudo déjà actif."}))
                return
            
            connected_clients[websocket] = username
            
            # Initialisation du profil avec sa couleur unique
            if username not in user_profiles:
                def_col = get_user_default_color(username)
                user_profiles[username] = {
                    "exp": 0, 
                    "level": 1, 
                    "default_color": def_col,
                    "color": def_col, 
                    "rank": "Novice"
                }
                save_state()
            else:
                if "default_color" not in user_profiles[username]:
                    user_profiles[username]["default_color"] = get_user_default_color(username)
                    save_state()
                
            await websocket.send(json.dumps({"type": "login_ok"}))
            await websocket.send(json.dumps({"type": "board_sync", "state": board_state}))
            for msg_dict in chat_history:
                await websocket.send(json.dumps(msg_dict))
            
            if session_running:
                await websocket.send(json.dumps({"type": "session_started"}))
                
            if username in dead_drops and dead_drops[username]:
                for drop in dead_drops[username]:
                    await websocket.send(json.dumps({
                        "type": "dead_drop",
                        "sender": drop["sender"],
                        "msg": drop["msg"]
                    }))
                dead_drops[username] = []
                save_state()
    except:
        return

    try:
        async for message in websocket:
            data = json.loads(message)
            username = connected_clients.get(websocket, "INCONNU")
            
            if data["type"] == "typing":
                broadcast_payload = json.dumps({
                    "type": "typing",
                    "username": username,
                    "status": data["status"]
                })
                for client in list(connected_clients.keys()):
                    if client != websocket:
                        try: await client.send(broadcast_payload)
                        except: pass
                continue

            elif data["type"] == "chat" and data["value"].startswith("/color"):
                parts = data["value"].split(" ")
                profile = user_profiles[username]
                def_col = profile.get("default_color", get_user_default_color(username))
                _, unlocks = get_rank_and_unlocks(profile["level"], def_col)
                
                if len(parts) < 2:
                    available = [k for k in unlocks.keys() if k != "default"]
                    await websocket.send(json.dumps({
                        "type": "chat", 
                        "username": "SYSTÈME", 
                        "color": "#ffaa00", 
                        "value": f"Couleurs débloquées : {', '.join(available)}"
                    }))
                    continue
                    
                requested_color = parts[1].lower()
                if requested_color in unlocks:
                    profile["color"] = unlocks[requested_color]
                    save_state()
                    await websocket.send(json.dumps({
                        "type": "chat", 
                        "username": "SYSTÈME", 
                        "color": "#ffaa00", 
                        "value": f"Couleur mise à jour avec succès."
                    }))
                else:
                    await websocket.send(json.dumps({
                        "type": "chat", 
                        "username": "SYSTÈME", 
                        "color": "#ff0055", 
                        "value": f"Couleur verrouillée ou inexistante à votre niveau."
                    }))
                continue

            elif data["type"] == "chat" and data["value"].startswith("/drop "):
                parts = data["value"].split(" ", 2)
                if len(parts) == 3 and parts[1].startswith("@"):
                    target = parts[1][1:].upper()
                    secret_msg = parts[2]
                    
                    dead_drops.setdefault(target, []).append({
                        "sender": username,
                        "msg": secret_msg
                    })
                    save_state()
                    
                    await websocket.send(json.dumps({
                        "type": "chat", 
                        "username": "SYSTÈME", 
                        "color": "#ffaa00", 
                        "value": f"Dead Drop crypté et stocké pour {target}."
                    }))
                else:
                    await websocket.send(json.dumps({"type": "chat", "username": "SYSTÈME", "color": "#ff0055", "value": "Syntaxe : /drop @Agent Ton message"}))
                continue

            elif data["type"] == "chat" and data["value"].startswith("/poll "):
                try:
                    parts = shlex.split(data["value"][6:])
                    if len(parts) < 3:
                        await websocket.send(json.dumps({"type": "chat", "username": "SYSTÈME", "color": "#ff0055", "value": "Syntaxe : /poll \"Question\" \"Choix 1\" \"Choix 2\""}))
                        continue
                    
                    active_poll = {"question": parts[0], "options": parts[1:], "votes": {}}
                    poll_text = generate_poll_text(active_poll)
                    await broadcast_system_msg(f"NOUVEAU SONDAGE PAR {username}\n{poll_text}", "#ffaa00")
                except Exception:
                    await websocket.send(json.dumps({"type": "chat", "username": "SYSTÈME", "color": "#ff0055", "value": "Erreur de syntaxe (vérifiez vos guillemets)."}))
                continue

            elif data["type"] == "chat" and data["value"].startswith("/vote "):
                if not active_poll:
                    await websocket.send(json.dumps({"type": "chat", "username": "SYSTÈME", "color": "#ff0055", "value": "Aucun sondage en cours."}))
                    continue
                
                parts = data["value"].split(" ")
                if len(parts) == 2 and parts[1].isdigit():
                    choix = int(parts[1]) - 1
                    if 0 <= choix < len(active_poll["options"]):
                        active_poll["votes"][username] = choix
                        poll_text = generate_poll_text(active_poll)
                        await broadcast_system_msg(f"MISE À JOUR DU SONDAGE :\n{poll_text}", "#ffaa00")
                    else:
                        await websocket.send(json.dumps({"type": "chat", "username": "SYSTÈME", "color": "#ff0055", "value": "Choix invalide."}))
                else:
                    await websocket.send(json.dumps({"type": "chat", "username": "SYSTÈME", "color": "#ff0055", "value": "Syntaxe : /vote 1"}))
                continue

            elif data["type"] == "chat" and data["value"].strip() == "/report":
                if not session_running:
                    await websocket.send(json.dumps({"type": "chat", "username": "SYSTÈME", "color": "#ff0055", "value": "Impossible : Aucune session en cours."}))
                    continue
                
                elapsed = SESSION_TOTAL_TIME - time_left
                minutes, seconds = divmod(elapsed, 60)
                duree_str = f"{minutes:02d}m {seconds:02d}s"
                session_running = False
                
                if not tasks_done_this_session:
                    report_text = f"RAPPORT DE SESSION\nInterrompue après {duree_str}."
                else:
                    completed_texts = [f" - {task['text']}" for task in board_state["tasks"] if task["id"] in tasks_done_this_session]
                    report_text = f"RAPPORT DE SESSION\nDurée : {duree_str}\nTâches accomplies :\n" + "\n".join(completed_texts)
                
                await broadcast_system_msg(report_text, "#ffaa00")
                
                stop_payload = json.dumps({"type": "session_stopped"})
                for client in list(connected_clients.keys()):
                    await client.send(stop_payload)
                continue

            elif data["type"] == "chat" and data["value"].startswith("/task "):
                parts = data["value"].split(" ", 2)
                if len(parts) >= 2:
                    cmd = parts[1].lower()
                    if cmd == "clear":
                        board_state["tasks"] = []
                        board_state["task_counter"] = 0
                        await broadcast_system_msg("Le tableau a été purgé.", "#ff0055")
                    elif cmd == "add" and len(parts) == 3:
                        board_state["tasks"].append({"id": board_state["task_counter"], "text": parts[2], "done": False, "assignee": "AUCUN"})
                        board_state["task_counter"] += 1
                    elif cmd == "assign" and len(parts) == 3:
                        subparts = parts[2].split(" ", 1)
                        if len(subparts) == 2 and subparts[0].startswith("@"):
                            board_state["tasks"].append({"id": board_state["task_counter"], "text": subparts[1], "done": False, "assignee": subparts[0][1:].upper()})
                            board_state["task_counter"] += 1
                    save_state()
                    sync_payload = json.dumps({"type": "board_sync", "state": board_state})
                    for client in list(connected_clients.keys()):
                        await client.send(sync_payload)
                continue

            elif data["type"] == "chat" and data["value"].startswith("/solve "):
                expression_str = data["value"][7:].strip()
                try:
                    transformations = (standard_transformations + (implicit_multiplication_application,))
                    expr = parse_expr(expression_str.replace("'", ""), transformations=transformations)
                    bot_reply = f"Simplification de {expression_str} : {sympy.simplify(expr)}"
                except Exception:
                    bot_reply = "Erreur : Syntaxe non reconnue."

                sys_dict = {"type": "chat", "username": "NEXUS_BOT", "color": "#ff00ff", "value": bot_reply}
                append_to_history(sys_dict)
                for client in list(connected_clients.keys()):
                    await client.send(json.dumps(sys_dict))
                continue
                
            elif data["type"] == "chat" and data["value"].startswith("/ping "):
                parts = data["value"].split(" ")
                if len(parts) >= 2:
                    ping_payload = json.dumps({"type": "ping", "sender": username, "target": parts[1].upper()})
                    for client in list(connected_clients.keys()):
                        await client.send(ping_payload)
                continue
            
            elif data["type"] == "chat":
                profile = user_profiles[username]
                msg_dict = {
                    "type": "chat", 
                    "username": f"[Lv.{profile['level']} {profile['rank']}] {username}", 
                    "color": profile["color"], 
                    "value": data["value"]
                }
                append_to_history(msg_dict)
                for client in list(connected_clients.keys()):
                    await client.send(json.dumps(msg_dict))
                continue

            elif data["type"] == "board_toggle":
                task_id = data["task_id"]
                for task in board_state["tasks"]:
                    if task["id"] == task_id:
                        was_done = task["done"]
                        task["done"] = not was_done 
                        save_state()
                        
                        if not was_done: 
                            if session_running:
                                tasks_done_this_session.add(task_id)
                                
                            leveled_up = add_exp(username, 10)
                            if leveled_up:
                                await broadcast_system_msg(f"PROMOTION : {username} a atteint le Niveau {user_profiles[username]['level']} ({user_profiles[username]['rank']}) !", "#ffaa00")
                                
                            if check_board_combo():
                                await broadcast_system_msg("SÉQUENCE COMPLÈTE : L'équipe a validé toutes les tâches de l'Intel Board !", "#ffaa00")
                        else:
                            if session_running and task_id in tasks_done_this_session:
                                tasks_done_this_session.remove(task_id)
                
                sync_payload = json.dumps({"type": "board_sync", "state": board_state})
                for client in list(connected_clients.keys()):
                    await client.send(sync_payload)
                continue

            elif data["type"] == "start_session":
                if not session_running:
                    session_running = True
                    time_left = SESSION_TOTAL_TIME
                    tasks_done_this_session.clear()
                    for client in list(connected_clients.keys()):
                        await client.send(json.dumps({"type": "session_started"}))
                continue

    finally:
        if websocket in connected_clients:
            del connected_clients[websocket]

async def main():
    print("=== SERVEUR NEXUS INITIALISÉ ===")
    load_state()
    asyncio.create_task(global_timer())
    
    # Render impose son propre port via les variables d'environnement
    port = int(os.environ.get("PORT", 8765))
    
    # On écoute sur 0.0.0.0 (toutes les interfaces) au lieu de 127.0.0.1
    async with websockets.serve(mission_control_hub, "0.0.0.0", port):
        await asyncio.Future()
if __name__ == "__main__":
    asyncio.run(main())
