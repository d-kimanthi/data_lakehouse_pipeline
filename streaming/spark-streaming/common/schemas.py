"""
Common schema definitions for e-commerce analytics.
"""

from pyspark.sql.types import (
    ArrayType,
    DecimalType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class EcommerceSchemas:
    """Container for all e-commerce event schemas"""

    @staticmethod
    def page_view_schema() -> StructType:
        """Schema for page view events"""
        return StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("page_type", StringType(), False),
                StructField("referrer", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("ip_address", StringType(), True),
                StructField("device_type", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

    @staticmethod
    def purchase_schema() -> StructType:
        """Schema for purchase events"""
        return StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("order_id", StringType(), False),
                StructField("total_amount", DecimalType(10, 2), False),
                StructField("subtotal", DecimalType(10, 2), False),
                StructField("discount_percent", IntegerType(), False),
                StructField("discount_amount", DecimalType(10, 2), False),
                StructField("payment_method", StringType(), False),
                StructField("shipping_method", StringType(), False),
                StructField(
                    "shipping_address",
                    StructType(
                        [
                            StructField("street", StringType(), True),
                            StructField("city", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("zip_code", StringType(), True),
                            StructField("country", StringType(), True),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "items",
                    ArrayType(
                        StructType(
                            [
                                StructField("product_id", StringType(), False),
                                StructField("quantity", IntegerType(), False),
                                StructField("unit_price", DecimalType(10, 2), False),
                                StructField("total_price", DecimalType(10, 2), False),
                                StructField("category", StringType(), False),
                                StructField("brand", StringType(), False),
                            ]
                        )
                    ),
                    False,
                ),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

    @staticmethod
    def add_to_cart_schema() -> StructType:
        """Schema for add to cart events"""
        return StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("price", DecimalType(10, 2), False),
                StructField("total_value", DecimalType(10, 2), False),
                StructField("cart_id", StringType(), False),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

    @staticmethod
    def user_session_schema() -> StructType:
        """Schema for user session events"""
        return StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("user_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("session_type", StringType(), False),
                StructField("device_type", StringType(), False),
                StructField("ip_address", StringType(), True),
                StructField("user_agent", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

    @staticmethod
    def product_update_schema() -> StructType:
        """Schema for product update events"""
        return StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("timestamp", TimestampType(), False),
                StructField("product_id", StringType(), False),
                StructField("update_type", StringType(), False),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )


# Convenience functions for easy import
def get_page_view_schema() -> StructType:
    """Get page view schema"""
    return EcommerceSchemas.page_view_schema()


def get_purchase_schema() -> StructType:
    """Get purchase schema"""
    return EcommerceSchemas.purchase_schema()


def get_add_to_cart_schema() -> StructType:
    """Get add to cart schema"""
    return EcommerceSchemas.add_to_cart_schema()


def get_user_session_schema() -> StructType:
    """Get user session schema"""
    return EcommerceSchemas.user_session_schema()


def get_product_update_schema() -> StructType:
    """Get product update schema"""
    return EcommerceSchemas.product_update_schema()
