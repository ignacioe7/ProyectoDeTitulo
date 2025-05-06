import asyncio
import random

async def smart_sleep(current_page: int, base_delay: float = 3.0):
  """
  Hacemos una pausa inteligente entre peticiones para que nos nos bloqueen en poco tiempo
  La idea es esperar un poquito más cada cierto número de páginas
  """
  delay = base_delay
  
  if current_page % 100 == 0:
    # Cada 100 páginas, una pausa más larga para relajar
    delay = random.uniform(55, 65) # Un minuto más o menos, aleatorio
    print(f"Pausa larga (página {current_page}): {delay:.2f} segundos")
  elif current_page % 50 == 0:
    # Cada 50 páginas, una pausa media
    delay = random.uniform(40, 50) # Unos 45 segundos, aleatorio
    print(f"Pausa media (página {current_page}): {delay:.2f} segundos")
  else:
    # Pausa normal, un poquito más larga cuanto más avanzamos
    # y con un toque aleatorio para parecer más humano
    extra_delay = (current_page // 10) * 0.1 # Un pequeño extra por cada 10 páginas
    delay += extra_delay + random.uniform(0.5, 1.5) # Sumamos el extra y algo aleatorio

  # Aseguramos que el delay mínimo sea el base_delay
  actual_delay = max(delay, base_delay)
  await asyncio.sleep(actual_delay)