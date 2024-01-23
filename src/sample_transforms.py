import logging
import pandas as pd


def flattenMongoDB(staging_area: dict[str, pd.DataFrame]):

    supplies_orders = staging_area['sales'].copy()
    supplies_orders['customer_email'] = supplies_orders['customer'].apply(lambda x: x['email'])
    supplies_orders['customer_gender'] = supplies_orders['customer'].apply(lambda x: x['gender'])
    supplies_orders['customer_age'] = supplies_orders['customer'].apply(lambda x: x['age'])
    supplies_orders['customer_satisfaction'] = supplies_orders['customer'].apply(lambda x: x['satisfaction'])

    supplies_orders.drop(['items', 'customer'], axis=1, inplace=True)
    staging_area['supplies_orders'] = supplies_orders

    supplies_order_items = staging_area['sales'].copy().explode('items')
    supplies_order_items['orderid'] = supplies_order_items.index
    supplies_order_items['name'] = supplies_order_items['items'].apply(lambda x: x['name'])
    supplies_order_items['price'] = supplies_order_items['items'].apply(lambda x: x['price'])
    supplies_order_items['quantity'] = supplies_order_items['items'].apply(lambda x: x['quantity'])
    
    supplies_order_items.drop(['items', 'customer', 'saleDate', 'storeLocation', 'couponUsed', 'purchaseMethod'], axis=1, inplace=True)
    supplies_order_items.reset_index(drop=True, inplace=True)
    supplies_order_items.index.rename('supplies_order_items_key', inplace=True)
    staging_area['supplies_order_items'] = supplies_order_items
    supplies_order_item_tags = staging_area['sales'].copy().explode('items')
    supplies_order_item_tags['name'] = supplies_order_item_tags['items'].apply(lambda x: x['name'])
    supplies_order_item_tags['tags'] = supplies_order_item_tags['items'].apply(lambda x: x['tags'])
    supplies_order_item_tags = supplies_order_item_tags.explode('tags')
    
    for i, row in supplies_order_item_tags.iterrows():
        logging.warning(f'Doing expensive calculation... Please stand by (reading index {i})')
        name = row['name']
        idx = supplies_order_items.query(f'name == "{ name }" and orderid == "{ i }"').index
        row['supplies_order_items_key'] = idx
    
    supplies_order_item_tags.reset_index(inplace=True)
    supplies_order_item_tags.index.rename('supplies_order_item_tag_key')
    supplies_order_item_tags.drop(['items', 'customer', 'saleDate', 'storeLocation', 'couponUsed', 'purchaseMethod', 'items', 'name'], axis=1, inplace=True)
    staging_area['supplies_order_item_tags'] = supplies_order_item_tags
    
    staging_area.pop('sales')
