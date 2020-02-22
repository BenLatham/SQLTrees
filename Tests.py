from AdjacencyTable import AdjacencyTable
import unittest
import sqlite3

class TestAdjacencyTable(unittest.TestCase):
    def _unpack(self, items):
        return [item[1] for item in items]

    def test_item_creation(self):
        at = AdjacencyTable("tests.sqlite3")
        at._create_drop()
        # list_items returns empty list for empty database
        self.assertEqual(at.list_items(), [])

        # one item added and shown with list items
        at.new_item("item 1")
        self.assertEqual(self._unpack(at.list_items()), ["item 1"])

        # duplicate item raises integrity error
        self.assertRaises(sqlite3.IntegrityError, at.new_item, "item 1")
        self.assertEqual(self._unpack(at.list_items()), ["item 1"])

        # multiple items added and shown with list items
        at.new_item("item 2")
        self.assertEqual(self._unpack(at.list_items()), ["item 1", "item 2"])

    def test_category_creation(self):
        at = AdjacencyTable("tests.sqlite3")
        at._create_drop()
        # Category added and list_items returns empty for empty category
        at.new_category("category 1")
        self.assertEqual(self._unpack(at.list_categories()), ["category 1"])
        self.assertEqual(at.list_items(1), [])

        # duplicate category raises integrity error
        self.assertRaises(sqlite3.IntegrityError, at.new_category, "category 1")
        self.assertEqual(self._unpack(at.list_categories()), ["category 1"])

    def test_categorising_items(self):
        at = AdjacencyTable("tests.sqlite3")
        at._create_drop()
        at.new_category("category 1")
        at.new_item("item 1")
        at.new_item("item 2")

        # list items returns empty for non-existent category
        self.assertEqual(at.list_items(2), [])

        # adding an item to a category
        at.categorise_item(1, 1)
        self.assertEqual(self._unpack(at.list_items()), ["item 1", "item 2"])
        self.assertEqual(self._unpack(at.list_items(1)), ["item 1"])

        # Invalid category raises exception
        self.assertRaises(sqlite3.IntegrityError, at.categorise_item, 1, "blah")

        # Invalid item raises exception
        self.assertRaises(ValueError, at.categorise_item, "blah", 1)

    def test_adding_subcategory(self):
        at = AdjacencyTable("tests.sqlite3")
        at._create_drop()
        at.new_category("category 1")
        at.new_item("item 1")
        at.new_item("item 2")
        at.categorise_item(1, 1)

        # adding a sub-category
        at.new_category("category 2", 1)
        at.new_item("item 3")
        at.categorise_item(3, 2)
        self.assertEqual(self._unpack(at.list_items(1)), ["item 1", "item 3"])
        self.assertEqual(self._unpack(at.list_items(2)), ["item 3"])

    def test_moving_categories(self):
        at = AdjacencyTable("tests.sqlite3")
        at._create_drop()
        at.new_category("category 1")
        at.new_category("category 2", 1)

        # category moves that produce loops are prevented
        self.assertFalse(at.move_category(1, 1))
        self.assertTrue(at.move_category(1, None))
        self.assertTrue(at.move_category(2, 1))
        self.assertFalse(at.move_category(1, 2))
        at.new_category("category 3", 1)
        at.new_category("category 4")
        self.assertTrue(at.move_category(1, 4))
        self.assertTrue(at.move_category(2, 4))

        # no-existent categories cannot be moved
        self.assertFalse(at.move_category(1000, 1))
        # category not moved to non-existant location
        self.assertFalse(at.move_category(1, 1000))

    def test_delete(self):
        at = AdjacencyTable("tests.sqlite3")
        at._create_drop()
        at.new_category("category 1")
        at.new_category("category 2")
        at.new_category("category 3", 1)
        at.new_category("category 4", 3)
        at.new_item("item 1")
        at.new_item("item 2")
        at.new_item("item 3")
        at.new_item("item 4")
        at.new_item("item 5")
        at.categorise_item(2, 1)
        at.categorise_item(3, 2)
        at.categorise_item(4, 3)
        at.categorise_item(5, 4)

        # delete an item
        self.assertEqual(self._unpack(at.list_items()), ["item 1", "item 2", "item 3", "item 4", "item 5"])
        at.delete_item(1)
        self.assertEqual(self._unpack(at.list_items()), ["item 2", "item 3", "item 4", "item 5"])

        # delete a category
        self.assertEqual(self._unpack(at.list_items(3)), ["item 4", "item 5"])
        at.delete_category(4)
        self.assertEqual(self._unpack(at.list_items(3)), ["item 4"])
        self.assertEqual(self._unpack(at.list_items()), ["item 2", "item 3", "item 4", "item 5"])

        # delete a category with subcategories
        at.delete_category(1)
        self.assertEqual(self._unpack(at.list_items()), ["item 2", "item 3", "item 4", "item 5"])
        self.assertEqual(self._unpack(at.list_items(3)), ["item 4"])



    def test_getting_children(self):
        at = AdjacencyTable("tests.sqlite3")
        at._create_drop()
        at.new_category("category 1")
        at.new_item("item 1")
        at.new_category("category 2", 1)
        at.new_item("item 2")
        at.categorise_item(2, 1)
        categories, items = at.list_children()
        self.assertEqual(self._unpack(categories), ["category 1"])
        self.assertEqual(self._unpack(items), ["item 1"])
        categories, items = at.list_children(1)
        self.assertEqual(self._unpack(categories), ["category 2"])
        self.assertEqual(self._unpack(items), ["item 2"])


if __name__ == '__main__':
    unittest.main()
