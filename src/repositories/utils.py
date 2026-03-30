from datetime import date
from sqlalchemy import select, func

from src.models.bookings import BookingsOrm
from src.models.rooms import RoomsOrm

def rooms_ids_for_booking(
    data_from: date,
    date_to: date,
    hotel_id: int | None = None,
):
    # 1) считаем сколько бронирований пересекают интервал по каждой комнате
    rooms_count = (
        select(BookingsOrm.room_id, func.count("*").label("rooms_booked"))
        .select_from(BookingsOrm)
        .filter(
            BookingsOrm.date_from <= date_to,
            BookingsOrm.date_to >= data_from,
        )
        .group_by(BookingsOrm.room_id)
        .cte(name="rooms_count")
    )

    # 2) считаем сколько осталось свободных мест
    rooms_left = (
        select(
            RoomsOrm.id.label("room_id"),
            (RoomsOrm.quantity - func.coalesce(rooms_count.c.rooms_booked, 0)).label("rooms_left"),
        )
        .select_from(RoomsOrm)
        .outerjoin(rooms_count, RoomsOrm.id == rooms_count.c.room_id)
        .cte(name="rooms_left")
    )

    # 3) фильтр по отелю (если нужен)
    rooms_in_hotel = select(RoomsOrm.id)
    if hotel_id is not None:
        rooms_in_hotel = rooms_in_hotel.filter(RoomsOrm.hotel_id == hotel_id)

    # 4) отдаём комнаты, где осталось > 0
    query = (
        select(rooms_left.c.room_id)
        .where(
            rooms_left.c.rooms_left > 0,
            rooms_left.c.room_id.in_(rooms_in_hotel),
        )
    )
    return query
