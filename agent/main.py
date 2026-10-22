"""Root entrypoint inside the AgentCore code zip (AgentCore runs `python main.py`)."""

from analyst.runtime import app

if __name__ == "__main__":
    app.run()
