select
    sq_ns.player1_acbl_number as ns1_acbl_number,
    sq_ns.player2_acbl_number as ns2_acbl_number,
    sq_ew.player1_acbl_number as ew1_acbl_number,
    sq_ew.player2_acbl_number as ew2_acbl_number,
    sq_ns.ns_match_points,
    sq_ns.ew_match_points,
    sq_ns.session_id
from

    /* Join board results and pairs for NS pairs */
    (select
        *
    from
        board_results b left join pairs p
        on 
            b.section_id = p.section_id and
            b.session_id = p.session_id and
            b.ns_pair = p.pair_number
        where p.direction = 'NS') sq_ns

    inner join

    /* Join board results and pairs for EW pairs */
    (select 
        *
    from
        board_results b left join pairs p 
        on
            b.section_id = p.section_id and
            b.session_id = p.session_id and
            b.ns_pair = p.pair_number
    where
        p.direction = 'EW') sq_ew

    on 
        sq_ns.board_id = sq_ew.board_id

;

