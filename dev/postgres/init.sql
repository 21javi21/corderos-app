-- Schema bootstrap for local Postgres used by the Corderos app
CREATE TABLE IF NOT EXISTS apuestas (
    id SERIAL PRIMARY KEY,
    apuesta VARCHAR(255) NOT NULL,
    creacion DATE NOT NULL,
    categoria VARCHAR(100) NOT NULL,
    tipo VARCHAR(100) NOT NULL,
    multiplica INT NOT NULL,
    apostante1 VARCHAR(100),
    apostante2 VARCHAR(100),
    apostante3 VARCHAR(100),
    apostado1 VARCHAR(255),
    apostado2 VARCHAR(255),
    apostado3 VARCHAR(255),
    ganador1 VARCHAR(100),
    ganador2 VARCHAR(100),
    perdedor1 VARCHAR(100),
    perdedor2 VARCHAR(100),
    locked BOOLEAN NOT NULL DEFAULT FALSE
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO corderos_app;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO corderos_app;
