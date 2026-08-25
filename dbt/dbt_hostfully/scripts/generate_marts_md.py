from google.cloud import bigquery
from datetime import datetime
client = bigquery.Client()

marts = ['dim_agencies','dim_airbnb_reviews','dim_booking_dot_com_reviews','dim_employees','dim_guests','dim_messages','dim_promo_codes','dim_properties','dim_property_amenities','dim_property_policies','dim_threads','fct_bookings','fct_order_transactions','fct_pricing']

md_lines = []
md_lines.append('# Hostfully — MARTS TABLES REFERENCE\n')
md_lines.append('Generated: %s\n' % datetime.utcnow().isoformat())
md_lines.append('Dataset: cloud-data-pipeline-projects.marts\n')

for t in marts:
    md_lines.append('---\n')
    md_lines.append('## %s\n' % t)
    desc = {
        'fct_bookings': 'Booking-level facts: one row per confirmed booking (revenue, dates, property, lead).',
        'fct_order_transactions': 'Payments and transactions tied to orders.',
        'fct_pricing': 'Daily pricing snapshots for property calendars.',
        'dim_properties': 'Property master data and channel listing statuses.',
        'dim_airbnb_reviews': 'Airbnb reviews with per-category rating breakdowns.',
        'dim_booking_dot_com_reviews': 'Booking.com reviews and ratings.',
        'dim_agencies': 'Agency information and regional details.',
        'dim_employees': 'Employee directory tied to agencies.',
        'dim_promo_codes': 'Promo codes metadata and details.',
        'dim_messages': 'Messages exchanged between leads/hosts/travelers.',
        'dim_threads': 'Thread level summary of messages and discussions.',
        'dim_property_amenities': 'Amenity flags and lists for properties.',
        'dim_property_policies': 'Property rules and policies.',
        'dim_guests': 'Guest profile information and counts.',
    }.get(t, '')
    md_lines.append('**Description:** %s\n' % desc)
    try:
        table = client.get_table('cloud-data-pipeline-projects.marts.' + t)
        md_lines.append('**Columns:**\n')
        for field in table.schema:
            md_lines.append('- `%s` (%s, %s)\n' % (field.name, field.field_type, field.mode))
        # sample analytical queries business friendly (avoid backticks to prevent shell issues)
        md_lines.append('\n**Sample analytical queries:**\n')
        if t == 'fct_bookings':
            md_lines.append('- Recent bookings: SELECT lead_id, property_id, booking_status, booking_amount FROM cloud-data-pipeline-projects.marts.fct_bookings ORDER BY order_updated_at_utc DESC LIMIT 20;\n')
            md_lines.append('- Revenue by day: SELECT DATE(order_updated_at_utc) AS day, COUNT(*) AS bookings, SUM(booking_amount) AS revenue FROM cloud-data-pipeline-projects.marts.fct_bookings GROUP BY day ORDER BY day DESC LIMIT 30;\n')
        elif t == 'fct_order_transactions':
            md_lines.append('- Total payments by currency: SELECT currency, SUM(amount) AS total FROM cloud-data-pipeline-projects.marts.fct_order_transactions GROUP BY currency;\n')
        elif t == 'fct_pricing':
            md_lines.append('- Average base rate per property: SELECT property_id, AVG(base_rate) AS avg_rate FROM cloud-data-pipeline-projects.marts.fct_pricing GROUP BY property_id ORDER BY avg_rate DESC LIMIT 10;\n')
        else:
            if t == 'dim_properties':
                md_lines.append('- Properties listed on Booking.com: SELECT property_id, public_property_name, booking_dot_com_status FROM cloud-data-pipeline-projects.marts.dim_properties WHERE booking_dot_com_status IS NOT NULL LIMIT 50;\n')
            if t == 'dim_airbnb_reviews':
                md_lines.append('- Average score per property: SELECT property_id, AVG(review_score_value) AS avg_score FROM cloud-data-pipeline-projects.marts.dim_airbnb_reviews GROUP BY property_id ORDER BY avg_score DESC LIMIT 20;\n')
            if t == 'dim_promo_codes':
                md_lines.append('- Promo code usage: SELECT p.promo_code, COUNT(f.order_id) AS uses FROM cloud-data-pipeline-projects.marts.dim_promo_codes p JOIN cloud-data-pipeline-projects.marts.fct_bookings f ON p.promo_code = f.promo_code GROUP BY p.promo_code ORDER BY uses DESC LIMIT 20;\n')
        # sample rows
        rows = client.list_rows(table, max_results=3).to_dataframe()
        if not rows.empty:
            md_lines.append('\n**Sample rows (values):**\n')
            for r in rows.head(3).to_dict(orient='records'):
                md_lines.append('- %s\n' % r)
        else:
            md_lines.append('\n**Sample rows (values):** None\n')
    except Exception as e:
        md_lines.append('ERROR fetching table: %s\n' % str(e))

out = '/opt/airflow/dbt_project/docs/MARTS_TABLES_REFERENCE.md'
with open(out, 'w') as f:
    f.write('\n'.join(md_lines))
print('WROTE', out)
