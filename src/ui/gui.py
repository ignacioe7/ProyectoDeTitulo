import tkinter as tk
from .cli import ScrapeAllGUI

class Application(tk.Tk):
    def __init__(self):
        super().__init__()
        self.scraper = ScrapeAllGUI()
        self._setup_ui()
    
    def _setup_ui(self):
        # Implementar interfaz gráfica aquí
        pass

if __name__ == "__main__":
    app = Application()
    app.mainloop()