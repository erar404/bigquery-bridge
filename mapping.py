table_mapping = {
    'CustomerPOULBQ_v1': {
        'customer_id': 'customerId',
        'po_ref_number': 'poRefNumber',
        'company_id': 'companyid',
        'warehouse_id': 'warehouseid',
        'po_date': 'poDate',
        'delivery_date': 'deliveryDate',
        'cancellation_date': 'cancellationDate',
        'customer_branch_id': 'customerBranchId',
        'customer_branch_name': 'customerBranchName',
        'customer_branch_lookup_code': 'customerBranchLookUpCode',
        'remark': 'remark',
        'customer_po_id': 'customerPOId',
        'po_status': 'poStatus',
        'manual_encoded': 'manualEncoded',
        'create_by': 'createBy',
        'create_date': 'createDate',
        'update_by': 'updateBy',
        'update_date': 'updateDate',
        'cancel_by': 'cancelBy',
        'cancel_date': 'cancelDate',
        'cancel_reason': 'cancelReason'
    }, 
    'CustomerPOULDetailBQ_v1': {
        'customer_po_detail_id': 'customerPODetailId',
        'customer_po_id': 'customerPOId',
        'item_id': 'itemId',
        'item_code': 'itemCode',
        'item_description': 'itemDescription',
        'uom': 'uom',
        'quantity': 'quantity',
        'unit_price': 'unitPrice',
        'discount_percent': 'discountPercent',
        'discount_amount': 'discountAmount',
        'total_amount': 'totalAmount',
        'create_by': 'createBy',
        'create_date': 'createDate',
        'update_by': 'updateBy',
        'update_date': 'updateDate'
    },
    'customerpoul_v2': {
        'company_name': 'companyName',
        'customer_name': 'customerName',
        'po_ref_number': 'poRefNumber',
        'po_date': 'poDate',
        'delivery_date': 'deliveryDate',
        'cancellation_date': 'cancellationDate',
        'customer_branch_name': 'customerBranchName',
        'remark': 'remark',
    },
    'customerpouldetail_v2': {
        'customer_name': 'customerName',
        'customer_branch_name': 'customerBranchName',
        'po_ref_number_primary': 'poRefNumber',
        'customer_sku_code': 'customerSKUCode',
        'customer_sku_desc': 'customerSKUDesc',
        'po_qty': 'poQty',
        'unit_price': 'unitPrice',
        'discount_percent': 'discountPercent',
        'net_price': 'netPrice'
    },
    'customerpoul_v3': {
        'company_name': 'companyName',
        'customer_name': 'customerName',
        'po_ref_number': 'poRefNumber',
        'po_date': 'poDate',
        'delivery_date': 'deliveryDate',
        'cancellation_date': 'cancellationDate',
        'customer_branch_name': 'customerBranchName',
        'remark': 'remark',
    },
    'customerpouldetail_v3': {
        'customer_name': 'customerName',
        'customer_branch_name': 'customerBranchName',
        'po_ref_number_primary': 'poRefNumber',
        'customer_sku_code': 'customerSKUCode',
        'customer_sku_desc': 'customerSKUDesc',
        'po_qty': 'poQty',
        'unit_price': 'unitPrice',
        'discount_percent': 'discountPercent',
        'net_price': 'netPrice'
    },
    'customerpoul_v4': {
        'po_ref_number': 'poRefNumber',
        'po_ref_number_primary': 'poRefNumberPrimary',
        'po_ref_number_count': 'poRefNumberCount',
        'file_name': 'fileName',
        'company_name': 'companyName',
        'customer_name': 'customerName',
        'customer_branch_name' : 'customerBranchName',
        'created_at' : 'createdAt',
        'delivery_date': 'deliveryDate',
        'po_date': 'poDate',
        'cancellation_date': 'cancellationDate', 
        'customer_sku_code': 'customerSKUCode',
        'customer_sku_desc': 'customerSKUDesc',
        'po_qty': 'poQty',
        'po_qty_pcs': 'poQtyPcs',
        'unit_price': 'unitPrice',
        'unit_price_pcs': 'unitPricePcs',
        'net_price': 'netPrice',
        'total_discount': 'totalDiscount',
        'total_discount_percent': 'totalDiscountPercent',
        'total_gross_amount': 'totalGrossAmount',
        'total_net_amount': 'totalNetAmount',
        'total_quantity': 'totalQuantity',
        'remark': 'remark',
    },
    'customerpouldetail_v4': {
        'po_ref_number_primary': 'poRefNumber',
        'customer_name': 'customerName',
        'customer_branch_name': 'customerBranchName',
        'created_at': 'createdAt',
        'po_date': 'poDate',
        'delivery_date': 'deliveryDate',
        'line_index': 'lineIndex',
        'customer_sku_code': 'customerSKUCode',
        'customer_sku_desc': 'customerSKUDesc',
        'po_qty': 'poQty',
        'po_qty_pcs': 'poQtyPcs',
        'unit_of_measurement' : 'unitOfMeasurement',
        'unit_price': 'unitPrice',
        'unit_price_pcs': 'unitPricePcs',
        'unit_amount': 'unitAmount',
        'net_amount': 'netAmount',
        'net_price': 'netPrice',
        'net_price_pcs': 'netPricePcs',
        'discount_percent': 'discountPercent',
    }
}

ignore_columns = {
    'customerpoul_v2': [
        'file_name', 
        'gcs_uri', 
        'processor_version',
        'created_at',
        'event_id',
        'generation',
        'size_bytes',
        'content_type',
        'md5_hash',
        'page_count',
        'is_multi_page',
        'po_ref_number_primary',
        'po_ref_number_count',
        'is_multi_po',
        'customer_sku_code',
        'customer_sku_desc',
        'po_qty',
        'po_qty_pcs',
        'unit_price',
        'unit_price_pcs',
        'net_price',
        'total_discount_percent',
        'entities_json',
        'total_discount',
    ],
    'customerpouldetail_v2': [
        'event_id',
        'file_name',
        'gcs_uri',
        'processor_version',
        'created_at',
        'page_count',
        'line_index',
        'po_qty_pcs',
        'unit_price_pcs',
        'po_ref_number_item',
        'delivery_date',
        'po_date',
        'md5_hash',
        'net_amount',
        'net_price_pcs',
        'unit_amount',
    ],
    'customerpoul_v3': [
        'created_at',
        'po_ref_number_primary',
        'po_ref_number_count',
        'customer_sku_code',
        'customer_sku_desc',
        'po_qty',
        'po_qty_pcs',
        'unit_price',
        'unit_price_pcs',
        'net_price',
        'total_discount_percent',
        'total_discount',
        'total_gross_amount',
        'total_net_amount',
        'total_quantity',
    ],
    'customerpouldetail_v3': [
        'created_at',
        'line_index',
        'po_qty_pcs',
        'unit_price_pcs',
        'delivery_date',
        'po_date',
        'net_amount',
        'net_price_pcs',
        'unit_amount',
        'unit_of_measurement'
    ],
    'customerpouldetail_v4': [
        'file_name',
    ]
}

column_defaults = {
    'customerpoul_v2': {
            'customer_name': '',
            'po_ref_number': '',
            'remark': ''
    },
    'customerpouldetail_v2': {
            'po_qty': 0,
            'unit_price': 0,
            'discount_percent': 0,
            'net_price': 0,
            'customer_name': '',
            'customer_sku_desc': '',
            'customer_branch_name': ''
    },
    'customerpoul_v3': {
            'customer_name': '',
            'po_ref_number': '',
            'remark': ''
    },
    'customerpouldetail_v3': {
            'po_qty': 0,
            'unit_price': 0,
            'discount_percent': 0,
            'net_price': 0,
            'customer_name': '',
            'customer_sku_desc': '',
            'customer_branch_name': ''
    },
    'customerpoul_v4': {
            'customer_name': '',
            'po_ref_number': '',
            'remark': ''
    },
    'customerpouldetail_v4': {
            'po_qty': 0,
            'unit_price': 0,
            'discount_percent': 0,
            'net_price': 0,
            'customer_name': '',
            'customer_sku_desc': '',
            'customer_branch_name': ''
    }
}

required_columns = {
    'customerpoul_v2': [
        'customer_name', 
        'po_ref_number'
    ],
    'customerpouldetail_v2': [
        'customer_name', 
        'po_ref_number_primary', 
        'customer_sku_code'
    ],
    'customerpoul_v3': [
        'customer_name', 
        'po_ref_number'
    ],
    'customerpouldetail_v3': [
        'customer_name', 
        'po_ref_number_primary', 
        'customer_sku_code'
    ],
    'customerpoul_v4': [
        'customer_name', 
        'po_ref_number'
    ],
    'customerpouldetail_v4': [
        'customer_name', 
        'po_ref_number_primary', 
        'customer_sku_code'
    ]
}

date_columns = {
    'customerpoul_v2': [
        'po_date', 
        'delivery_date', 
        'cancellation_date'
    ],
    'customerpoul_v3': [
        'po_date', 
        'delivery_date', 
        'cancellation_date'
    ],
    'customerpoul_v4': [
        'po_date', 
        'delivery_date', 
        'cancellation_date'
    ]
}

original_columns = {
    'customerpoul_v4': [
        'po_ref_number',
        'po_ref_number_primary',
        'po_ref_number_count',
        'company_name',
        'customer_name',
        'customer_branch_name',
        'created_at',
        'delivery_date',
        'po_date',
        'cancellation_date',
        'customer_sku_code',
        'customer_sku_desc',
        'po_qty',
        'po_qty_pcs',
        'unit_price',
        'unit_price_pcs',
        'net_price',
        'total_discount',
        'total_discount_percent',
        'total_gross_amount',
        'total_net_amount',
        'total_quantity',
        'remark',
    ],
    'customerpouldetail_v4': [
        'po_ref_number_primary',
        'customer_name',
        'customer_branch_name',
        'created_at',
        'po_date',
        'delivery_date',
        'line_index',
        'customer_sku_code',
        'customer_sku_desc',
        'po_qty',
        'po_qty_pcs',
        'unit_of_measurement',
        'unit_price',
        'unit_price_pcs',
        'unit_amount',
        'net_amount',
        'net_price',
        'net_price_pcs',
        'discount_percent'
    ]
}
