from singer import metrics, utils, log_info
from tap_brightpearl.context import Context



class Stream():

    resource = {
        "brand": {"url_path": "product-service/brand-search"},
        "product": {"url_path": "product-service/product-search"},

        "product_with_custom": {"url_path": "product-service/product",
                                "depending_on": "product-service/product",
                                "search_param": {"includeOptional":"customFields"}},
        "product_price": {"url_path": "/product-service/product-price/",
                          "depending_on": "product-service/product"},

        "brightpearl_category": {"url_path": "product-service/brightpearl-category-search"},
        "collection": {"url_path": "product-service/collection-search"},
        "option": {"url_path": "product-service/option-search"},
        "option_value": {"url_path": "product-service/option-value-search"},
        "price_list": {"url_path": "product-service/price-list/"},

        "custom_field_meta_data": {"url_path": "product-service/product/custom-field-meta-data"},
        "product_type": {"url_path": "product-service/product-type-search"},
        "channel_brand": {"url_path": "/product-service/channel-brand"},
        "channel": {"url_path": "/product-service/channel"},

        "order_search": {"url_path": "order-service/order-search"},
        "orders": {"url_path": "order-service/order", "depending_on": "order-service/order",
                   "search_param": {"includeOptional": "customFields,nullCustomFields"}},

        # thos can be optional due to orders
        "sales_order": {"url_path": "order-service/sales-order", "depending_on": "order-service/sales-order"},
        "sales_credit": {"url_path": "order-service/sales-credit", "depending_on": "order-service/sales-credit"},

        "order_type": {"url_path": "order-service/order-type"},
        "order_status": {"url_path": "order-service/order-status"},
        "order_stock_status": {"url_path": "order-service/order-stock-status"},
        "order_shipping_status": {"url_path": "order-service/order-shipping-status"},
        "order_custom_field_meta_data_purchase": {"url_path": "order-service/purchase/custom-field-meta-data"},
        "order_custom_field_meta_data_sale": {"url_path": "order-service/sale/custom-field-meta-data"},
        "purchase_order_landed_cost": {"url_path": "order-service/purchase-order-lc-search"},
        "order_note": {"url_path": "order-service/order-note-search"},

        "location": {"url_path": "warehouse-service/location-search"},
        "zone": {"url_path": "warehouse-service/zone-search"},
        "warehouse": {"url_path": "warehouse-service/warehouse-search"},
        "shipping_method": {"url_path": "warehouse-service/shipping-method-search"},
        "goods_movement": {"url_path": "warehouse-service/goods-movement-search"},
        "product_availability": {"url_path": "/warehouse-service/product-availability/",
                                 "depending_on": "product-service/product",
                                 "search_param": {"includeOptional": "breakDownByLocation"},
                                 "schema": {"product_id": "integer", "stock": "object"}},

        "tax_code": {"url_path": "accounting-service/tax-code"},
        "supplier_payment": {"url_path": "accounting-service/supplier-payment-search"},
        "journal": {"url_path": "accounting-service/journal-search"},
        "exchange_rate": {"url_path": "accounting-service/exchange-rate"},
        "customer_payment": {"url_path": "accounting-service/customer-payment-search"},
        "currency": {"url_path": "accounting-service/currency-search"},
        "accounting_period": {"url_path": "accounting-service/accounting-period"},

        "company": {"url_path": "contact-service/company-search"},
        "contact": {"url_path": "contact-service/contact-search"},
        "contact_group": {"url_path": "contact-service/contact-group-search"},
        "contact_group_member": {"url_path": "contact-service/contact-group-member-search"},
        "lead_source": {"url_path": "contact-service/lead-source"},
        "goods_out_search": {"url_path": "/warehouse-service/goods-note/goods-out-search"},
        "goods_in_search": {"url_path": "/warehouse-service/goods-in-search"},
        "goods_note_out": {"url_path": "/warehouse-service/order/",
                           "url_extension": "/goods-note/goods-out/",
                           "depending_on": "order-service/order",
                           "schema": {"goods_note_id": "integer", "goods_note": "object"},
                           },
        "goods_note_in": {"url_path": "/warehouse-service/order/",
                          "url_extension": "/goods-note/goods-in/",
                          "depending_on": "order-service/order",
                          "schema": {"goods_note_id": "integer", "goods_note": "object"},
                          },
    }

    def __init__(self, entity):
        self.entity = entity

    def get_data(self, first_result=1, discovery=False):
        url_path = self.resource[self.entity]['url_path']

        lastResult = 500 if discovery else None

        if "depending_on" in self.resource[self.entity]:
            get_urls = Context.session.get_data(url_path=self.resource[self.entity]["depending_on"],
                                     firstResult=first_result, lastResult=lastResult, method="OPTIONS")

            for url in get_urls["getUris"]:
                # getting values from url only
                # /product/1,2,4-8
                search_param = {}
                if "search_param" in self.resource[self.entity]:
                    search_param = self.resource[self.entity]["search_param"]

                values = url.split("/")[2]
                if "url_extension" in self.resource[self.entity]:
                    build_url_path = url_path + values + self.resource[self.entity]["url_extension"]
                else:
                    build_url_path = url_path+"/"+values

                log_info("Processing dependent URL:" + build_url_path)
                data = Context.session.get_data(url_path=build_url_path,
                                                firstResult=first_result,
                                                lastResult=lastResult,
                                                search_params=search_param)
                if data:
                    yield data

                # run just for one batch to get the schema
                if discovery:
                    break

        else:
            data = Context.session.get_data(url_path=url_path,
                                            firstResult=first_result, lastResult=lastResult)
            yield data

    def get_schema(self):
        cols = {}
        with metrics.http_request_timer(self.entity):
            # schema pre-defined
            if "schema" in self.resource[self.entity]:
                for key in self.resource[self.entity]["schema"]:
                    cols[key] = {"type": [self.resource[self.entity]["schema"][key]]}
            else:
                for data in self.get_data(discovery=True):

                    if "metaData" in data:
                        metadata = data["metaData"]
                        for col in metadata["columns"]:
                            if col["reportDataType"] == "PERIOD" or col["reportDataType"] == "SEARCH_STRING":
                                data_type = "string"
                            elif col["reportDataType"] == "IDSET":
                                data_type = "integer"
                            else:
                                data_type = col["reportDataType"].lower()

                            cols[col["name"]] = {"type": ["null", data_type]}
                    else:
                        for d in data:
                            for key in d:
                                value = d[key]
                                type = None

                                if isinstance(value, str):
                                    type = 'string'
                                if isinstance(value, list):
                                    type = 'string'
                                if isinstance(value, dict):
                                    type = 'object'
                                if isinstance(value, int):
                                    type = 'integer'
                                if isinstance(value, bool):
                                    type = 'boolean'

                                if type is None:
                                    type = 'string'

                                cols[key] = {"type": ["null", type]}

        return cols

    def sync(self):
        firstResult = 1
        keep_going = True
        merge_col_names = False
        while True:
            with metrics.http_request_timer(self.entity):
                for data in self.get_data(first_result=firstResult):
                    if "results" in data:
                        merge_col_names = True
                        objects = data["results"]
                        metadata = data["metaData"]

                        cols = []
                        for col in metadata["columns"]:
                            cols.append(col["name"])

                        if metadata["morePagesAvailable"]:
                            firstResult = metadata["lastResult"] + 1
                            keep_going = True
                        else:
                            keep_going = False

                    elif "schema" in self.resource[self.entity]:
                    # key value style
                        objects = []
                        col_key = list(self.resource[self.entity]["schema"])[0]
                        col_value = list(self.resource[self.entity]["schema"])[1]
                        keep_going = False
                        for key in data:
                            objects.append({ col_key: key, col_value: data[key] })

                    else:
                        objects = data
                        keep_going = False

                    for obj in objects:
                        obj_data = dict(zip(cols, obj)) if merge_col_names else obj
                        yield obj_data

                if not keep_going:
                    break
