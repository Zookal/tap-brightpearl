from singer import metrics, utils, log_info, logger
from tap_brightpearl.context import Context


class Stream():
    resource = {
        ## attribute tables
        "brand": {"url_path": "product-service/brand-search"},
        "collection": {"url_path": "product-service/collection-search"},
        "option": {"url_path": "product-service/option-search"},
        "option_value": {"url_path": "product-service/option-value-search"},
        "price_list": {"url_path": "product-service/price-list/"},
        "product_type": {"url_path": "product-service/product-type-search"},
        "channel_brand": {"url_path": "/product-service/channel-brand"},
        "channel": {"url_path": "/product-service/channel"},

        "order_type": {"url_path": "order-service/order-type"},
        "order_status": {"url_path": "order-service/order-status"},
        "order_stock_status": {"url_path": "order-service/order-stock-status"},
        "order_shipping_status": {"url_path": "order-service/order-shipping-status"},

        "company": {"url_path": "contact-service/company-search"},
        "tax_code": {"url_path": "accounting-service/tax-code"},
        "exchange_rate": {"url_path": "accounting-service/exchange-rate"},
        "currency": {"url_path": "accounting-service/currency-search"},
        "accounting_period": {"url_path": "accounting-service/accounting-period"},

        "location": {"url_path": "warehouse-service/location-search"},
        "zone": {"url_path": "warehouse-service/zone-search"},
        "warehouse": {"url_path": "warehouse-service/warehouse-search"},
        "shipping_method": {"url_path": "warehouse-service/shipping-method-search"},

        ## guide for data deletion
        "product_idset": {"url_path": "product-service/product", "method": "options"},
        "order_idset": {"url_path": "order-service/order", "method": "options"},

        ## incremental load
        "brightpearl_category": {"url_path": "product-service/brightpearl-category-search",
                                 "state_filter": "updatedOn"},
        "product": {"url_path": "product-service/product-search",
                    "state_filter":"updatedOn"},

        "contact": {"url_path": "contact-service/contact-search",
                    "state_filter":"updatedOn"},

        "goods_movement": {"url_path": "warehouse-service/goods-movement-search",
                           "state_filter":"updatedOn"},

        "customer_payment": {"url_path": "accounting-service/customer-payment-search",
                             "state_filter":"createdOn"},

        "journal": {"url_path": "accounting-service/journal-search",
                    "state_filter":"journalDateEntered"},

        "order_search": {"url_path": "order-service/order-search",
                           "state_filter":"updatedOn"},

        "goods_out_search": {"url_path": "/warehouse-service/goods-note/goods-out-search",
                             "state_filter":"createdOn"},

        "goods_in_search": {"url_path": "/warehouse-service/goods-in-search",
                            "state_filter":"createdDate"},

        ## incremental dynamic
        "orders": {"url_path": "order-service/order", "depending_on": "order-service/order",
                   "depending_on_incremental": "order-service/order-search",
                   "depending_on_incremental_state_filter": "updatedOn",
                   "search_param": {"includeOptional": "customFields,nullCustomFields"}},

        "goods_note_out": {"url_path": "/warehouse-service/order/",
                           "url_extension": "/goods-note/goods-out/",
                           "depending_on": "order-service/order",
                           "depending_on_incremental": "order-service/order-search",
                           "depending_on_incremental_state_filter": "updatedOn",
                           "schema": {"goods_note_id": "integer", "goods_note": "object"},
                           },
        "goods_note_in": {"url_path": "/warehouse-service/order/",
                          "url_extension": "/goods-note/goods-in/",
                          "depending_on": "order-service/order",
                          "depending_on_incremental": "order-service/order-search",
                          "depending_on_incremental_state_filter": "updatedOn",
                          "schema": {"goods_note_id": "integer", "goods_note": "object"},
                          },

        ## slow
        "product_with_custom": {"url_path": "product-service/product",
                                "depending_on": "product-service/product",
                                "depending_on_incremental":  "product-service/product-search",
                                "depending_on_incremental_state_filter": "updatedOn",
                                "search_param": {"includeOptional":"customFields"}
                                },

        "product_price": {"url_path": "/product-service/product-price/",
                          "depending_on": "product-service/product",
                          "depending_on_incremental":  "product-service/product-search",
                          "depending_on_incremental_state_filter": "updatedOn"
                          },

        "purchase_order_landed_cost": {"url_path": "order-service/purchase-order-lc-search"},


        ####
        "custom_field_meta_data": {"url_path": "product-service/product/custom-field-meta-data"},


        # those can be optional due to orders
        "sales_order": {"url_path": "order-service/sales-order", "depending_on": "order-service/sales-order"},
        "sales_credit": {"url_path": "order-service/sales-credit", "depending_on": "order-service/sales-credit"},

        "order_custom_field_meta_data_purchase": {"url_path": "order-service/purchase/custom-field-meta-data"},
        "order_custom_field_meta_data_sale": {"url_path": "order-service/sale/custom-field-meta-data"},
        "order_note": {"url_path": "order-service/order-note-search"},

        "product_availability": {"url_path": "/warehouse-service/product-availability/",
                                 "depending_on": "product-service/product",
                                 "search_param": {"includeOptional": "breakDownByLocation"},
                                 "schema": {"product_id": "integer", "stock": "object"}},

        "supplier_payment": {"url_path": "accounting-service/supplier-payment-search"},

        "contact_group": {"url_path": "contact-service/contact-group-search"},
        "contact_group_member": {"url_path": "contact-service/contact-group-member-search"},
        "lead_source": {"url_path": "contact-service/lead-source"},
    }

    def __init__(self, entity):
        self.entity = entity

    def get_uris(self, first_result=1, lastResult=500, discovery=False):
        """
        Either get the URI list from a proper service on BP API or
        build a mimic one with incremental from the search emdpoints

        BP IDSET [OPTIONS} endpoint does not have a search capability to limit the size of records to fetch.
        This will might help to decrease the processing time using the Search endpoint and rebuild the list of uri.

        :param first_result:
        :param lastResult:
        :return:
        """

        if discovery or "depending_on_incremental" not in self.resource[self.entity]:
            get_urls = Context.session.get_data(url_path=self.resource[self.entity]["depending_on"],
                                                firstResult=first_result, lastResult=lastResult, method="OPTIONS")
        else:
            counter = 0
            max = 200
            temp_urls = []
            get_urls_array = []

            state_filter = {}
            state_filter_field = None
            if "depending_on_incremental_state_filter" in self.resource[self.entity]:
                state_filter_field = self.resource[self.entity]["depending_on_incremental_state_filter"]
                state_last_updated_at = Context.get_state_value(self.entity, self.resource[self.entity]["depending_on_incremental_state_filter"])
                last_updated_at = state_last_updated_at
                if state_last_updated_at:
                    state_filter[state_filter_field] = f"{state_last_updated_at}/"

            while True:
                data = Context.session.get_data(url_path=self.resource[self.entity]["depending_on_incremental"],
                                                    firstResult=first_result, lastResult=lastResult,
                                                    search_params=state_filter)

                metadata = data["metaData"]
                index_state_field = 0
                for col in data["metaData"]["columns"]:
                    if col["name"] == state_filter_field:
                        break
                    index_state_field += 1

                for d in data["results"]:
                    counter += 1
                    order_id = d[0]
                    obj_date = d[index_state_field]
                    temp_urls.append(str(order_id))

                    if obj_date > last_updated_at:
                        last_updated_at = obj_date

                    if counter == max:
                        get_urls_array.append("/order/"+",".join(temp_urls))
                        temp_urls = []
                        counter = 0

                if metadata["morePagesAvailable"]:
                    first_result = metadata["lastResult"] + 1
                else:
                    break

            if len(temp_urls) > 0:
                get_urls_array.append("/order/"+",".join(temp_urls))

            if state_filter_field:
                Context.set_state_value(self.entity, state_filter_field, last_updated_at)

            logger.log_info("Orders to be process:"+str(metadata["resultsAvailable"]))
            get_urls = {"getUris":get_urls_array}

        return get_urls


    def get_data(self, first_result=1, discovery=False, state_filter={}):
        url_path = self.resource[self.entity]['url_path']

        lastResult = 500 if discovery else None

        search_param = {}
        if "search_param" in self.resource[self.entity]:
            search_param = self.resource[self.entity]["search_param"]

        if "depending_on" in self.resource[self.entity]:
            get_urls = self.get_uris(first_result, lastResult, discovery)

            for url in get_urls["getUris"]:
                # getting values from url only
                # /product/1,2,4-8
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
            if state_filter:
                search_param.update(state_filter)

            data = Context.session.get_data(url_path=url_path,
                                            firstResult=first_result,
                                            lastResult=lastResult,
                                            search_params=search_param,
                                            method=self.resource[self.entity].get("method","GET")
                                            )
            yield data

    def get_schema(self):
        cols = {}
        with metrics.http_request_timer(self.entity):
            # schema pre-defined
            if "schema" in self.resource[self.entity]:
                for key in self.resource[self.entity]["schema"]:
                    cols[key] = {"type": [self.resource[self.entity]["schema"][key]]}

            elif self.resource[self.entity].get("method","").lower() == "options":
                cols["getUris"] = {"type": ["null", "string"]}

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

        state_filter={}
        state_filter_field=None
        if "state_filter" in self.resource[self.entity]:
            state_filter_field = self.resource[self.entity]["state_filter"]
            state_last_updated_at = Context.get_state_value(self.entity, self.resource[self.entity]["state_filter"])
            last_updated_at = state_last_updated_at
            if state_last_updated_at:
                state_filter[state_filter_field] = f"{state_last_updated_at}/"

        while True:
            with metrics.http_request_timer(self.entity):
                for data in self.get_data(first_result=firstResult, state_filter=state_filter):
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

                    elif "getUris" in data:
                        merge_col_names = False
                        objects = []
                        for x in data["getUris"]:
                            objects.append({"getUris": x})
                        keep_going = False

                    else:
                        objects = data
                        keep_going = False

                    for obj in objects:
                        obj_data = dict(zip(cols, obj)) if merge_col_names else obj

                        if state_filter_field:
                            obj_date = obj_data[state_filter_field] if obj_data[state_filter_field] else obj_data.get("createdOn", state_filter_field)
                            if obj_date > last_updated_at:
                                last_updated_at = obj_data[state_filter_field]

                        yield obj_data

                if not keep_going:
                    break

        if state_filter_field:
            Context.set_state_value(self.entity, state_filter_field, last_updated_at)