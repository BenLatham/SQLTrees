import sqlite3


class AdjacencyTable:
    """
    An implementation of a tree using the Adjacency Table approach in sqlite. The tree contains two types of nodes,
    to represent items and categories. Categories can contain other categories and items, while items can only be leaf
    nodes.
    The following operations can be carried out:

    - adding new items (use new_item)
    - adding new categories (use new_category)
    - adding categories as subcategories of another category (use new_category)
    - assigning/reassigning items to a category (use categorise_item)
    - moving categories into another category (use move_category)
    - list all items that are descendants of a given node (use list_items)
    - list the direct children of a category, including both categories and items (use list_children)
    - list all categories (use list_categories)
    """

    def __init__(self, database):
        """
        :Description: Create or open the named database, as an sqlite3 database and turns on foreign key constraints
        :param database: The name of the database to be used
        """
        self.db = sqlite3.connect(database)
        self.db.cursor().execute("PRAGMA foreign_keys = ON;")
        self.db.commit()

    def new_item(self, name):
        """
        :Description: Create a new item
        :param name: The name of the item you wish to create. Must be unique.
        :return: None
        """
        cur = self.db.cursor()
        statement = """
        INSERT INTO AT_item (name) VALUES (?);
        """
        cur.execute(statement, [name])
        self.db.commit()
        cur.close()

    def new_category(self, name, parent=None):
        """
        :Description: Create a new category
        :param name: Category Name. Must be unique
        :param parent: Containing category if the new category is a sub category
        :return: None
        """
        cur = self.db.cursor()
        statement = """
        INSERT INTO AT_Category (name, parent) VALUES(?, ?);
        """
        cur.execute(statement, [name, parent])
        self.db.commit()
        cur.close()

    def categorise_item(self, item, category):
        """
        :Description: Move an item into a category
        :param item: The id of the item
        :param category: The id of the category
        :return: None
        """
        cur = self.db.cursor()
        statement = """
        UPDATE AT_Item
        SET category = ?
        WHERE id =?;
        """
        cur.execute(statement, [category, item])
        if not cur.rowcount == 1:
            raise ValueError("Item Does Not Exist")
        self.db.commit()
        cur.close()

    def move_category(self, category, destination):
        """
        :Description: Make one category a child of another category. Creation of loops is not permitted.
            All descendants of the moved category are now also descendants of the destination category.
            Setting destination to none will move the category to root.
        :param category: The id of the category you wish to move
        :param destination: The id of the category you wish to make the category a child of
        :return: a boolean declaring whether the category was successfully moved. Returns False if one of the
            categories given as parameters is not valid, or when attempting to make a category a child of one of it's
            own descendants.
        """
        cur = self.db.cursor()
        if destination is None:
            script = """
                     UPDATE AT_Category SET parent=NULL WHERE id=:category;
                     """
        else:
            script = """
                    UPDATE AT_Category SET parent=:destination WHERE id=:category  
                    AND NOT (
                    WITH RECURSIVE ancestors AS
                    (
                      SELECT parent, id=:category AS match FROM AT_Category WHERE id=:destination
                      UNION ALL
                      SELECT c.parent, c.id=:category AS match FROM AT_Category c JOIN ancestors
                      ON ancestors.parent=c.id AND NOT ancestors.match
                    )
                    SELECT MAX(match) AS loop FROM ancestors);
                    """
        cur.execute(script, {"category": category, "destination": destination})
        data = cur.rowcount
        self.db.commit()
        cur.close()
        if data == 1:
            return True
        return False

    def list_items(self, category=None):
        """
        :Description: List the id and name of all items in the given category or its descendants.
            If the category is None then all items are listed
        :param category: The id of the category
        :return: A list of the ids and names of items within the category
        """
        if category is None:
            return self._all_items()
        else:
            return self._items_in_category(category)

    def list_categories(self):
        """
        :Description: Get a list of the ids and names of all categories
        :return: a list of the ids and names of all categories
        """
        cur = self.db.cursor()
        script = """
        SELECT id, name from AT_Category ORDER BY name;
        """
        cur.execute(script)
        data = cur.fetchall()
        cur.close()
        return data

    def list_children(self, parent=None):
        """
        :Description: List all the direct children of a category
        :param parent: The parent category, defaults to None returning items and categories at the root of the tree
        :return: a list of the ids and names of the categories that are direct children
            and a list of the ids and names of the items that are direct children of the given category
        """
        cur = self.db.cursor()
        if parent is not None:
            statement = """SELECT id, name from AT_Category WHERE parent=?;"""
            cur.execute(statement, (parent,))
            categories = cur.fetchall()

            statement = """SELECT id, name from AT_Item WHERE category=?;"""
            cur.execute(statement, (parent,))
            items = cur.fetchall()
        else:
            statement = """SELECT id, name from AT_Category WHERE parent IS NULL;"""
            cur.execute(statement)
            categories = cur.fetchall()
            statement = """SELECT id, name from AT_Item WHERE category IS NULL;"""
            cur.execute(statement)
            items = cur.fetchall()
        cur.close()
        return categories, items

    def delete_category(self, category):
        """
        :Description: Delete a category, moving it's direct children to the root of the tree
        :param category: The category to delete
        :return: Boolean indicating whether delete was successful
        """
        cur = self.db.cursor()
        statement = """DELETE FROM AT_Category WHERE id=?"""
        cur.execute(statement, [category])
        self.db.commit()
        rows_changed = cur.rowcount
        cur.close()
        return rows_changed == 1

    def delete_item(self, item):
        """
        :Description: Delete an item
        :param item: The item to delete
        :return: Boolean indicating whether delete was successful
        """
        cur = self.db.cursor()
        statement = """DELETE FROM AT_Item WHERE id=?"""
        cur.execute(statement, [item])
        self.db.commit()
        rows_changed = cur.rowcount
        cur.close()
        return rows_changed == 1

    def _all_items(self):
        cur = self.db.cursor()
        script = """
        SELECT id, name from AT_Item ORDER BY AT_Item.name;
        """
        cur.execute(script)
        data = cur.fetchall()
        cur.close()
        return data

    def _items_in_category(self, category):
        cur = self.db.cursor()
        script = """
        WITH RECURSIVE cte AS
        (
          SELECT ? AS id
          UNION ALL
          SELECT c.id FROM AT_Category c JOIN cte
          ON cte.id=c.parent
        )
        SELECT AT_Item.id, AT_Item.name
        FROM cte INNER JOIN AT_Item
        ON category = cte.id
        ORDER BY name;
        """

        cur.execute(script, [category])
        data = cur.fetchall()
        self.db.commit()
        cur.close()
        return data

    def _create_drop(self):
        cur = self.db.cursor()
        script = """
           DROP INDEX IF EXISTS CategoryName;
           DROP INDEX IF EXISTS ItemName;
           DROP TABLE IF EXISTS AT_Item;
           DROP TABLE IF EXISTS AT_Category;


           CREATE TABLE AT_Category(
             id INTEGER PRIMARY KEY,
             name TEXT NOT NULL,
             parent INTEGER,          
             FOREIGN KEY (parent) REFERENCES AT_Category(id) ON DELETE SET NULL
           );
           CREATE UNIQUE INDEX CategoryName ON AT_Category(name);

           CREATE TABLE AT_Item(
             id INTEGER PRIMARY KEY,
             name TEXT NOT NULL,
             category INTEGER,
             FOREIGN KEY (category) REFERENCES AT_Category(id) ON DELETE SET NULL         
           );
           CREATE UNIQUE INDEX ItemName ON AT_Item(name);
           """
        cur.executescript(script)
        cur.close()
