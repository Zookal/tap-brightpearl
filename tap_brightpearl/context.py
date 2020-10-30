import singer
from singer import metadata
from datetime import datetime, timedelta

LOGGER = singer.get_logger()

class Context():
    config = {}
    state = {}
    catalog = {}
    schema = {}
    stream_map = {}
    stream_objects = {}
    counts = {}
    session = None

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]: s for s in cls.catalog['streams']}
        return cls.stream_map[stream_name]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        stream_metadata = metadata.to_map(stream['metadata'])
        return metadata.get(stream_metadata, (), 'selected')

    @classmethod
    def get_results_per_page(cls, default_results_per_page):
        results_per_page = default_results_per_page
        try:
            results_per_page = int(cls.config.get("results_per_page"))
        except TypeError:
            # None value or no key
            pass
        except ValueError:
            # non-int value
            log_msg = ('Failed to parse results_per_page value of "%s" ' +
                       'as an integer, falling back to default of %d')
            LOGGER.info(log_msg,
                        Context.config['results_per_page'],
                        default_results_per_page)
        return results_per_page

    @classmethod
    def get_bookmark(cls, stream_name):
        return cls.state.get('bookmarks', {}).get(stream_name, {})

    @classmethod
    def get_state_value(cls, stream_name, field):
        bookmark = cls.get_bookmark(stream_name)
        back_in_days = Context.config.get("incremental_back_days",0)
        state_value = bookmark.get(field, "")
        if state_value:
            DT_FORMAT="%Y-%m-%dT%H:%M:%S.%f%z"
            state_value = (datetime.strptime(state_value,DT_FORMAT)-timedelta(days=back_in_days)).strftime(DT_FORMAT)
        return state_value

    @classmethod
    def set_state_value(cls, stream_name, field, value):
        cls.state['bookmarks'].update({stream_name:{field:value}})

