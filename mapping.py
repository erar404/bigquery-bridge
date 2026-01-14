

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
        'entities_json',
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
        'entities_json',
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
    ]
}