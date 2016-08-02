DROP database IF EXISTS stock;
CREATE database stock DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

GRANT ALL PRIVILEGES ON stock.* TO 'stock'@'127.0.0.1' IDENTIFIED BY 'stock';

CREATE IF NOT EXISTS stock (
    number  VARCHAR(30) NOT NULL,
    name    VARCHAR(30) NOT NULL,
    begin   DATE NOT NULL
)
Engine=InnoDB;
