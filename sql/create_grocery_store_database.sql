CREATE TABLE product_nodes
(
    node_id                VARCHAR(36)  DEFAULT uuid()
  , node_natural_key       INTEGER      NOT NULL
  , node_name              VARCHAR(100) NOT NULL
  , level_name             VARCHAR(100) NOT NULL
  , parent_node_id         VARCHAR(36)
--
  , CONSTRAINT product_nodes_pk PRIMARY KEY (node_id)
  , CONSTRAINT product_nodes_uk_1 UNIQUE (node_natural_key)
  , CONSTRAINT product_nodes_self_fk FOREIGN KEY (parent_node_id)
        REFERENCES product_nodes (node_id)
)
;

-- Root (Top) Node (has no parent node)
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (0, 'All Products', 'Total Products', NULL);

-- Produce Category Level Node
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (10, 'Produce', 'Category', (SELECT node_id
                                      FROM product_nodes
                                     WHERE node_name = 'All Products'));

-- Produce Category Children Leaf-Level Nodes
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (101, 'Spinach', 'UPC', (SELECT node_id
                                 FROM product_nodes
                                WHERE node_name = 'Produce'));

INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (102, 'Tomatoes', 'UPC', (SELECT node_id
                                   FROM product_nodes
                                  WHERE node_name = 'Produce'));

-- Candy Category Level Node
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (20, 'Candy', 'Category', (SELECT node_id
                                   FROM product_nodes
                                  WHERE node_name = 'All Products'));

-- Candy Category Children Leaf-Level Nodes
INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (201, 'Hershey Bar', 'UPC', (SELECT node_id
                                    FROM product_nodes
                                   WHERE node_name = 'Candy'));

INSERT INTO product_nodes (node_natural_key, node_name, level_name, parent_node_id)
VALUES (202, 'Nerds', 'UPC', (SELECT node_id
                              FROM product_nodes
                             WHERE node_name = 'Candy'));

/* ------------------------------------------------------------------------------------ */

CREATE TABLE sales_facts (
  product_id    INTEGER NOT NULL
, customer_id   VARCHAR (100) NOT NULL
, date_id       DATE    NOT NULL
, unit_quantity NUMERIC NOT NULL
, sales_amount  NUMERIC NOT NULL
)
;

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Hershey Bar')
      , 'Phil'
      , DATE '2022-01-01'
      , 1
      , 3.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Hershey Bar')
      , 'Lottie'
      , DATE '2022-01-02'
      , 5
      , 15.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Nerds')
      , 'Kalie'
      , DATE '2022-01-02'
      , 2
      , 5.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Tomatoes')
      , 'Phil'
      , DATE '2022-01-02'
      , 2
      , 2.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Spinach')
      , 'Popeye'
      , DATE '2022-01-03'
      , 10
      , 5.00
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Spinach')
      , 'Brutus'
      , DATE '2022-01-04'
      , 1
      , 0.50
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Spinach')
      , 'Lottie'
      , DATE '2022-01-04'
      , 1
      , 0.50
       );

INSERT INTO sales_facts (product_id, customer_id, date_id, unit_quantity, sales_amount)
VALUES ((SELECT node_natural_key
           FROM product_nodes
          WHERE node_name = 'Spinach')
      , 'Phil'
      , DATE '2022-01-05'
      , 2
      , 2.00
       );
