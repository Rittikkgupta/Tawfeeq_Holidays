from django.urls import path
from login.views import *

urlpatterns = [
    path('verify/', Verify_User.as_view(), name='verify_user'),
    path('bookings/', Bookings.as_view(), name='bookings'),
    path('bookingdetails/', BookingDetails.as_view(), name='bookingdetails'),
    path('costprice/', costprice.as_view(), name='costprice'),
    path('transfer_assign_detail/', TransferAssignDetail.as_view(), name='transfer_assign_detail'),
    path('driver_duty/', DriverDuty.as_view(), name='driver_duty'),
    path('service_details_for_email/', ServiceDetailsForEmail.as_view(), name='service_details_for_email'),
    path('priortime/', Priortime.as_view(), name='priortime'),
    path('assigntransfers_getcostprice/', AssignTransfersGetCostPrice.as_view(), name='assigntransfers_getcostprice'),
    path('Transfers_dashboard_final/', TransfersDashboardFinal.as_view(), name='Transfers_dashboard_final'),
    path('transfer_List_new_cnaupc/', TransferListNewCnaupc.as_view(), name='transfer_List_new_cnaupc'),
   #path('Transferdate/', Transferdate.as_view(), name='Transferdate'),
]