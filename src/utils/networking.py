import asyncio
import random

# ===============================================================
# PAUSA INTELIGENTE
# ===============================================================

async def smart_sleep(current_page: int, base_delay: float = 3.0):
  # PAUSA INTELIGENTE PARA EVITAR BLOQUEOS DE TRIPADVISOR
  # Incrementa delays progresivamente y añade pausas largas en intervalos
  # Ayuda a mantener el scraping sin ser detectado como bot
  delay = base_delay 
  
  if current_page % 100 == 0:
    # pausa larga cada 100 páginas para descansar
    delay = random.uniform(55, 65) 
  elif current_page % 50 == 0:
    # pausa media cada 50 páginas 
    delay = random.uniform(40, 50) 
  else:
    # pausa normal con incremento gradual + aleatorio
    extra_delay = (current_page // 10) * 0.1 # sube cada 10 páginas
    delay += extra_delay + random.uniform(0.5, 1.5) # suma random

  # nunca menos del mínimo
  actual_delay = max(delay, base_delay)
  await asyncio.sleep(actual_delay)