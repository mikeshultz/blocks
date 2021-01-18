from blocks.conductor.api import app, api, init_flask  # noqa: F401

init_flask()

if __name__ == "__main__":
    api()
