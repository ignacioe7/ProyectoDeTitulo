# MÓDULO DE UTILIDADES DE RED PARA SCRAPING INTELIGENTE
# Implementa pausas adaptativas para evitar detección anti-bot
# Proporciona delays escalados según progreso y patrones de uso

import asyncio
import random

# ====================================================================================================================
#                                         PAUSA INTELIGENTE ANTI-DETECCIÓN
# ====================================================================================================================

async def smart_sleep(current_page: int, base_delay: float = 3.0):
  # IMPLEMENTA PAUSAS ESCALADAS PARA EVITAR BLOQUEO POR TRIPADVISOR
  # Ajusta delay según número de página procesada con intervalos específicos
  # Usa aleatorización para simular comportamiento humano natural
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