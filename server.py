import asyncio
import websockets
import json
import random
import math
import time

# ── Настройки ───────────────────────────────────────────
HOST = "0.0.0.0"
PORT = 8765

MAP_COLS = 40
MAP_ROWS = 25
DAY_LENGTH = 3600
TICK_RATE = 1/60  # 60 тиков в секунду

MAP = [
    [3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3],
    [3,3,3,3,2,2,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,2,2,2,1,1,3,3,3,3,3,3,3,3],
    [3,3,3,2,2,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,2,1,0,0,1,3,3,3,3,3,3,3],
    [3,3,2,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0,0,0,0,1,3,3,3,3,3,3],
    [3,2,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,3,3,3,3,3],
    [3,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,3,3,3,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,2,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,2,2,3],
    [3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,2,2,2,3],
    [3,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,2,2,2,2,3],
    [3,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,2,2,2,2,2,3],
    [3,3,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,2,2,2,2,2,2,3],
    [3,3,3,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,2,2,2,2,2,2,3,3],
    [3,3,3,3,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,2,2,2,2,2,2,2,3,3,3],
    [3,3,3,3,3,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,2,2,2,2,2,2,3,3,3,3,3],
    [3,3,3,3,3,3,3,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,2,2,2,2,2,3,3,3,3,3,3,3,3],
    [3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3],
]

# ── Состояние мира ──────────────────────────────────────
world = {
    "players": {},        # {player_id: {x, y, name, resources}}
    "buildings": {},      # {"col,row": building_id}
    "mutants": [],        # [{x, y, hp, max_hp, target}]
    "day_tick": 0,
    "epoch": 1,
    "epoch_progress": 0,
}

# Ресурсные узлы
resource_nodes = {}

def init_nodes():
    random.seed(42)
    node_types = [
        {"type":"wood","amount":5,"label":"Дерево"},
        {"type":"stone","amount":8,"label":"Камень"},
        {"type":"metal","amount":3,"label":"Металл"},
    ]
    counts = {"wood":18,"stone":14,"metal":8}
    for ntype, count in counts.items():
        placed=0; attempts=0
        ni=next(n for n in node_types if n["type"]==ntype)
        while placed<count and attempts<1000:
            attempts+=1
            col=random.randint(2,MAP_COLS-3)
            row=random.randint(2,MAP_ROWS-3)
            if MAP[row][col]==0 and f"{col},{row}" not in resource_nodes:
                amt=ni["amount"]+random.randint(-1,2)
                resource_nodes[f"{col},{row}"]={
                    "type":ntype,"amount":amt,"max":amt,
                    "label":ni["label"],"regen_timer":0
                }
                placed+=1

init_nodes()

# ── Подключённые клиенты ────────────────────────────────
clients = {}  # {websocket: player_id}
player_counter = 0

# ── Логика мутантов ─────────────────────────────────────
def spawn_mutant():
    edges = []
    for col in range(MAP_COLS):
        for row in range(MAP_ROWS):
            if MAP[row][col]==0:
                if any(0<=nc<MAP_COLS and 0<=nr<MAP_ROWS and MAP[nr][nc]==3
                       for nc,nr in [(col+1,row),(col-1,row),(col,row+1),(col,row-1)]):
                    edges.append((col,row))
    if not edges: return
    col,row=random.choice(edges)
    world["mutants"].append({
        "x":float(col*48+24),"y":float(row*48+24),
        "hp":3,"max_hp":3,"target":None,"attack_timer":0,
        "id": random.randint(10000,99999)
    })

def update_mutants():
    t = world["day_tick"] / DAY_LENGTH
    night_alpha = max(0, min(200, int((math.cos(2*math.pi*t)+1)/2*180)))

    if night_alpha > 100 and random.randint(0, 90) == 0:
        spawn_mutant()

    for m in world["mutants"][:]:
        buildings = world["buildings"]
        if not m["target"] or m["target"] not in buildings:
            if buildings:
                m["target"] = min(buildings.keys(),
                    key=lambda pos: (int(pos.split(",")[0])-m["x"]/48)**2 +
                                    (int(pos.split(",")[1])-m["y"]/48)**2)
            else:
                m["target"] = None

        if m["target"]:
            tc,tr = map(int, m["target"].split(","))
            tx = tc*48+24; ty = tr*48+24
            dx,dy = tx-m["x"], ty-m["y"]
            dist = max(1,(dx**2+dy**2)**0.5)
            if dist > 48*0.8:
                m["x"]+=dx/dist*0.8; m["y"]+=dy/dist*0.8
            else:
                m["attack_timer"]+=1
                if m["attack_timer"]>=90:
                    m["attack_timer"]=0
                    # Урон зданию — просто удаляем после N ударов
                    # (упрощённо для сервера)

def update_world():
    world["day_tick"] = (world["day_tick"]+1) % DAY_LENGTH

    # Регенерация ресурсов
    for pos, node in resource_nodes.items():
        if node["amount"] <= 0:
            node["regen_timer"] += 1
            if node["regen_timer"] >= DAY_LENGTH*2:
                node["regen_timer"] = 0
                node["amount"] = node["max"]

    update_mutants()

# ── Обработка сообщений от клиентов ────────────────────
async def handle_message(ws, player_id, msg):
    try:
        data = json.loads(msg)
        action = data.get("action")

        if action == "move":
            col = data.get("col", 10)
            row = data.get("row", 9)
            if player_id in world["players"]:
                world["players"][player_id]["col"] = col
                world["players"][player_id]["row"] = row

        elif action == "build":
            col = data.get("col")
            row = data.get("row")
            building = data.get("building")
            key = f"{col},{row}"
            if key not in world["buildings"] and MAP[row][col] != 3:
                world["buildings"][key] = building

        elif action == "demolish":
            key = data.get("key")
            if key in world["buildings"]:
                del world["buildings"][key]

        elif action == "gather":
            key = data.get("key")
            res_type = data.get("type")
            if key in resource_nodes and resource_nodes[key]["amount"] > 0:
                resource_nodes[key]["amount"] -= 1
                # Отправляем игроку подтверждение
                await ws.send(json.dumps({
                    "type": "gather_ok",
                    "resource": res_type
         elif action == "set_name":
             name = data.get("name", f"Игрок")
             if player_id in world["players"]:
                world["players"][player_id]["name"] = name
                }))

    except Exception as e:
        print(f"Ошибка обработки сообщения: {e}")

# ── Отправка состояния мира всем клиентам ───────────────
async def broadcast_state():
    if not clients:
        return
    state = {
        "type": "state",
        "players": world["players"],
        "buildings": world["buildings"],
        "mutants": world["mutants"],
        "day_tick": world["day_tick"],
        "epoch": world["epoch"],
        "nodes": {k: {"amount": v["amount"]} for k,v in resource_nodes.items()},
    }
    msg = json.dumps(state)
    dead = []
    for ws in clients:
        try:
            await ws.send(msg)
        except:
            dead.append(ws)
    for ws in dead:
        await disconnect(ws)

# ── Подключение / отключение ────────────────────────────
async def disconnect(ws):
    if ws in clients:
        pid = clients[ws]
        del clients[ws]
        if pid in world["players"]:
            del world["players"][pid]
        print(f"Игрок {pid} отключился. Онлайн: {len(clients)}")

async def handler(ws):
    global player_counter
    player_counter += 1
    player_id = f"player_{player_counter}"
    clients[ws] = player_id

    # Добавляем игрока в мир
    world["players"][player_id] = {
        "col": 10, "row": 9,
        "name": f"Игрок {player_counter}",
        "color": [random.randint(100,255), random.randint(100,255), random.randint(100,255)]
    }

    print(f"Игрок {player_id} подключился. Онлайн: {len(clients)}")

    # Отправляем начальное состояние
    await ws.send(json.dumps({
        "type": "init",
        "player_id": player_id,
        "map": MAP,
        "nodes": resource_nodes,
        "state": {
            "players": world["players"],
            "buildings": world["buildings"],
            "day_tick": world["day_tick"],
        }
    }))

    try:
        async for msg in ws:
            await handle_message(ws, player_id, msg)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await disconnect(ws)

# ── Главный игровой цикл ────────────────────────────────
async def game_loop():
    tick = 0
    while True:
        update_world()
        tick += 1
        # Отправляем состояние каждые 3 тика (20 раз в секунду)
        if tick % 3 == 0:
            await broadcast_state()
        await asyncio.sleep(TICK_RATE)

# ── Запуск ──────────────────────────────────────────────
async def main():
    print(f"Сервер запущен на {HOST}:{PORT}")
    async with websockets.serve(handler, HOST, PORT):
        await game_loop()

if __name__ == "__main__":
    asyncio.run(main())
