using BusComun;
using Comun;
using System;
using System.Data;

namespace BusInchost
{
    public class TyPago
    {
        //CAMPOS IN
        public string PGOBLIG;
        public string PGFECVAL;
        public string PGFECOPER;
        public string PGFECVTO;
        public string PGFECENT;
        public string PGMONEDA;
        public string PGTIPO;
        public string PGSUBTIPO;
        public string PGSIGNO;
        public double PGTOTAL;
        public double PGCAPITAL;
        public double PGINTCOR;
        public double PGIMPUEST;
        public double PGGASCOB;
        public string PGCOMPROBANTE;
        public string PGNOTA;
        public string PGIDPAGO;
        public string PGHISTORICO;

        //CAMPOS CALCULADOS
        public string PGFECSIS;
        public string PGPERFIL;
        public string PGNMORA;

        public string sOblgConv;
        public double nConv;

        public int Compromiso;

    }

    public class TySaldoCP
    {

        public double CUCONV;
        public double CUCUOTA;
        public double CUTOTAL;
        public double CUSALDOPEND;
        public string CUOBLG;
        public double CUSALDOPENDCUOTA;
    }

    public class TyPagoConvP
    {
        public double PGCONV;
        public string PGIDPAGO;
        public int PGCUOTACONV;
        public double PGTOTAL;
        public string PGFECVAL;
    }

    public class TyRCPago
    {
        public string CPOBLIG;
        public string CPIDPROM;
        public string CPFECVEN;
        public double CPCANCOM;
        public double CPCANPAG;
        public string CPPERFIL;
    }

    public class PagosDalc : cBase
    {
        public PagosDalc(cConexion pconn)
        {
            conn = pconn;
        }

        /// <summary>
        /// Recupera todos los pagos de Hoy ordenados por obligación y fecha valor
        /// </summary>
        public AISDataReader AbrirCursorNuevosPagos()
        {
            AISDataReader Dr = new AISDataReader();

            string sSql = $@" SELECT PGOBLIG, PGFECVAL, PGFECOPER, PGFECVTO, PGFECENT, PGMONEDA, PGTIPO, PGSUBTIPO, PGSIGNO, 
 case PGTIPO when 'REV' then  PGTOTAL * -1 else PGTOTAL end as PGTOTAL, PGCAPITAL, PGINTCOR, PGIMPUEST, PGGASCOB, PGCOMPROBANTE, PGNOTA, PGIDPAGO, PGPERFIL,
   cab.COCONV Convenio, cab.COOBLGCONV OblgConv, 
   (select count(*) from RCPAGO where CPOBLIG = PGOBLIG and CPFECVEN >= PGFECVAL and CPCANCOM-CPCANPAG > 0 and CPFECNEG <= PGFECVAL AND CPHISTORICO ='0' ) as Compromiso
FROM RPAGOS
   LEFT JOIN RCONVP cab ON cab.COOBLGCONV = PGOBLIG and  cab.COESTADO = '1'
WHERE PGFECSIS = {cFormat.StToBD(cGlobales.Hoy)}
   AND (
       EXISTS (select 1 from RCPAGO where CPOBLIG = PGOBLIG and CPFECVEN >= PGFECVAL and CPCANCOM-CPCANPAG > 0 and CPFECNEG <= PGFECVAL AND CPHISTORICO ='0' ) 
       or EXISTS (select 1 from RCONVP cab2 where cab2.COOBLGCONV = PGOBLIG and  cab2.COESTADO = '1') 
   )
ORDER BY PGOBLIG, PGFECVAL ";

            Dr = conn.EjecutarDataReader(sSql);

            Errores = conn.Errores;

            return Dr;
        }

        public bool FechPago(AISDataReader DrP, TyPago Pago)
        {
            try
            {
                if (DrP.Read())
                {
                    Pago.PGOBLIG = DrP["PGOBLIG"].ToString();
                    Pago.PGFECVAL = DrP["PGFECVAL"].ToString();
                    Pago.PGFECOPER = DrP["PGFECOPER"].ToString();
                    Pago.PGFECVTO = DrP["PGFECVTO"].ToString();
                    Pago.PGFECENT = DrP["PGFECENT"].ToString();
                    Pago.PGMONEDA = DrP["PGMONEDA"].ToString();
                    //Pago.PGTIPO = DrP["PGTIPO"].ToString();
                    //Pago.PGSUBTIPO = DrP["PGSUBTIPO"].ToString();
                    //Pago.PGSIGNO = DrP["PGSIGNO"].ToString();
                    Pago.PGTOTAL = cFormat.NumBDToPC(DrP["PGTOTAL"].ToString());
                    //Pago.PGCAPITAL = cFormat.NumBDToPC(DrP["PGCAPITAL"].ToString());
                    //Pago.PGINTCOR = cFormat.NumBDToPC(DrP["PGINTCOR"].ToString());
                    //Pago.PGIMPUEST = cFormat.NumBDToPC(DrP["PGIMPUEST"].ToString());
                    //Pago.PGGASCOB = cFormat.NumBDToPC(DrP["PGGASCOB"].ToString());
                    //Pago.PGCOMPROBANTE = DrP["PGCOMPROBANTE"].ToString();
                    //Pago.PGNOTA = DrP["PGNOTA"].ToString();
                    Pago.PGIDPAGO = DrP["PGIDPAGO"].ToString();
                    //Pago.PGPERFIL = DrP["PGPERFIL"].ToString();
                    //Pago.PGNMORA = DrP["PGIDPAGO"].ToString();
                    //Pago.PGHISTORICO = "0";
                    //Pago.PGFECSIS = cGlobales.Hoy;
                    
                    //Para el cruce con Convenios
                    Pago.nConv = (DrP["Convenio"].ToString() != "") ? Convert.ToDouble(DrP["Convenio"].ToString()) : 0;
                    Pago.sOblgConv = DrP["OblgConv"].ToString();
                    Pago.Compromiso = Convert.ToInt32(DrP["Compromiso"].ToString());

                    return true;
                }
                else
                    return false;

            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechPago", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHPAGOS", "Error al recuperar Pago");
                return false;
            }
        }


        public bool ObligacionBajoConvenio(TyPago Pago)
        {
            DataSet Ds = new DataSet();

            string sSql = $@" SELECT cab.COCONV Convenio, cab.COOBLGCONV ObligConv 
FROM RCONVPOBLG oblg
JOIN RCONVP cab ON cab.COCONV = oblg.COCONV 
WHERE oblg.COOBLG = {cFormat.StToBD(Pago.PGOBLIG)} 
    AND cab.COESTADO ='1' ";

            Ds = conn.EjecutarQuery(sSql, "RCONVP");
            if (conn.Errores.Cantidad() == 0)
            {
                if (Ds.Tables["RCONVP"].Rows.Count > 0)
                {
                    Pago.nConv = Convert.ToDouble(Ds.Tables["RCONVP"].Rows[0]["Convenio"].ToString());
                    Pago.sOblgConv = Ds.Tables["RCONVP"].Rows[0]["ObligConv"].ToString();
                    return true;
                }
                else
                    return false;
            }
            else
            {
                Errores = conn.Errores;
                cIncidencia.Generar(Errores, "ObligacionBajoConvenio", "Error al recuperar Obligación bajo Convenio");
                return false;
            }
        }

        public DataSet AbrirCursorPendPagoCuotasConv(double nConv)
        {
            DataSet Dr = new DataSet();

            string sSql = $@" SELECT CUCONV, CUCUOTA, CUTOTAL, CUSALDOPEND, CUOBLG ,CUSALDOPENDCUOTA
             FROM RCUOTASCONVP 
             WHERE CUCONV= {nConv}
             AND CUSALDOPENDCUOTA > 0 
             ORDER BY CUCUOTA ";

            Dr = conn.EjecutarQuery(sSql, "RCUOTASCONVP");

            Errores = conn.Errores;
            return Dr;
        }

        public bool FetchPendPagoCuotasConv(DataRow Dr, TySaldoCP vSaldoCP)
        {
            try
            {
                vSaldoCP.CUCONV = cFormat.NumBDToPC(Dr["CUCONV"].ToString().Trim());
                vSaldoCP.CUCUOTA = cFormat.NumBDToPC(Dr["CUCUOTA"].ToString().Trim());
                vSaldoCP.CUTOTAL = cFormat.NumBDToPC(Dr["CUTOTAL"].ToString().Trim());
                vSaldoCP.CUSALDOPEND = cFormat.NumBDToPC(Dr["CUSALDOPEND"].ToString().Trim());
                vSaldoCP.CUSALDOPENDCUOTA = cFormat.NumBDToPC(Dr["CUSALDOPENDCUOTA"].ToString().Trim());
                vSaldoCP.CUOBLG = Dr["CUOBLG"].ToString().Trim();

                return true;
            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FetchPendPagoCuotasConv", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHCUOTCONV", "Error al recuperar Cuotas Convenio");
                return false;
            }
        }

        public bool ActualizoSaldoCuotaConvBD(TySaldoCP vSaldoCP)
        {
            int nRet;
            String sSql;

            sSql =$@" UPDATE RCUOTASCONVP 
                         SET CUSALDOPENDCUOTA = { cFormat.NumToBD( vSaldoCP.CUSALDOPENDCUOTA.ToString()) } 
                         WHERE CUCONV = { vSaldoCP.CUCONV.ToString() }
                         AND CUCUOTA = { vSaldoCP.CUCUOTA.ToString()} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizoSaldoCuotaConvBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDSACON", "Error al actualizar saldo convenio " + vSaldoCP.CUCONV.ToString());
                return false;
            }
        }

        public bool InsertoPagoConvenioBD(TyPagoConvP PagoConvP)
        {
            String sSql;
            int nRet;

            sSql = $@"INSERT INTO RPAGOSCONVP(PGCONV, PGIDPAGO, PGCUOTACONV,PGTOTAL,PGFECVAL)VALUES(
                             { PagoConvP.PGCONV.ToString() },
                            { cFormat.StToBD(PagoConvP.PGIDPAGO) },
                            { PagoConvP.PGCUOTACONV },
                            { cFormat.NumToBD(PagoConvP.PGTOTAL.ToString())},
                              { cFormat.StToBD(PagoConvP.PGFECVAL)}  )";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertoPagoConvenioBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSPAGCONV", "Error insertando pagos del convenio " + PagoConvP.PGCONV.ToString());
                return false;
            }
        }

        public void RecuperarUsuarioPagoEstandarBD(TyPago Pago)
        {
            try
            {
                Pago.PGPERFIL = "";
                Pago.PGNMORA = "";

                DataSet Ds = new DataSet();

                string sSql = $@" SELECT LOPERFIL, LONMORA
FROM ROBLG
JOIN RLOTE ON  LOCOD = OGLOTE
                WHERE OGCOD = {cFormat.StToBD(Pago.PGOBLIG)} ";

                Ds = conn.EjecutarQuery(sSql, "RDUENOLOTE");

                if (Ds.Tables["RDUENOLOTE"].Rows.Count > 0)
                {
                    Pago.PGPERFIL = Ds.Tables["RDUENOLOTE"].Rows[0]["LOPERFIL"].ToString().Trim();
                    Pago.PGNMORA = Ds.Tables["RDUENOLOTE"].Rows[0]["LONMORA"].ToString().Trim();
                }
                return;
            }
            catch
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "RecuperarUsuarioPagoEstandarBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "USUPAGO", "No se encontró usuario para el pago de la obligación " + Pago.PGOBLIG);
                return;
            }

        }

        public void RecuperarUsuarioPagoBD(TyPago Pago)
        {
            try
            {
                Pago.PGPERFIL = "";
                Pago.PGNMORA = "";

                DataSet Ds = new DataSet();

                string sSql = $@" SELECT DISTINCT HESOWNER, HPERFIL, LONMORA, HFECHADESDE 
FROM ROBLG 
JOIN RLOTE ON LOCOD = OGLOTE
JOIN RHCARTERA_ASIG ON HLOTE = LOCOD, 
RPARAM
WHERE OGCOD = {cFormat.StToBD(Pago.PGOBLIG)}
  AND HFECHADESDE <= {cFormat.StToBD(Pago.PGFECVAL)}
  AND HFECHAHASTA >= {cFormat.StToBD(Pago.PGFECVAL)}
  AND ( (HESOWNER='0' AND TO_CHAR(TO_DATE(HFECHADESDE,'YYYYMMDD')+PRDELAYPAGOS,'YYYYMMDD') >= {cFormat.StToBD(Pago.PGFECVAL)} ) 
       OR  (HESOWNER='1') 
      ) 
ORDER BY HFECHADESDE DESC ";

                Ds = conn.EjecutarQuery(sSql, "RDUENOLOTE");
                if (Ds.Tables["RDUENOLOTE"].Rows.Count > 0)
                {
                    Pago.PGPERFIL = Ds.Tables["RDUENOLOTE"].Rows[0]["HPERFIL"].ToString().Trim();
                    Pago.PGNMORA = Ds.Tables["RDUENOLOTE"].Rows[0]["LONMORA"].ToString().Trim();
                }
                return;
            }
            catch
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "RecuperarUsuarioPagoBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "USUPAGO1", "No se encontró usuario para el pago de la obligación " + Pago.PGOBLIG);
                return;
            }

        }

        public DataSet AbrirCursorRCPagosBD(TyRCPago vRCPago)
        {
            DataSet Ds = new DataSet();

            string sSql = $@" SELECT CPIDPROM, CPFECVEN, CPCANCOM,CPCANPAG,CPPERFIL 
             FROM RCPAGO 
             WHERE CPOBLIG = {cFormat.StToBD(vRCPago.CPOBLIG)}
               AND CPFECVEN >= {cFormat.StToBD(vRCPago.CPFECVEN)}
               AND CPCANCOM-CPCANPAG > 0 
               AND CPFECNEG <= {cFormat.StToBD(vRCPago.CPFECVEN)}
               AND CPHISTORICO ='0' 
             ORDER BY CPFECVEN ";

            Ds = conn.EjecutarQuery(sSql, "RCPAGO");

            Errores = conn.Errores;

            return Ds;
        }

        public bool ActualizaRCPagoBD(TyRCPago vRCPago, string pgfecval)
        {
            int nRet;
            String sSql;

            sSql = "UPDATE RCPAGO ";
            sSql += " SET CPCANPAG=" + cFormat.NumToBD(vRCPago.CPCANPAG.ToString()) + " , ";
            sSql += " CPFECCUMP = (CASE WHEN (" + cFormat.NumToBD(vRCPago.CPCANPAG.ToString()) + "/CPCANCOM)*100 >= " + cFormat.NumToBD(cGlobales.PctMinEficCP.ToString()) + " THEN " + pgfecval + " ELSE TO_NUMBER(CPFECCUMP) END)  ";
            sSql += " WHERE CPIDPROM = " + cFormat.StToBD(vRCPago.CPIDPROM);

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizaRCPagoBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDRCPAGO", "Error al actualizar promesa de pago " + vRCPago.CPIDPROM);
                return false;
            }
        }

        internal bool InsertaPagoBD(TyPago Pago)
        {
            int nRet;

            string sSql = $@" INSERT INTO RPAGOS (PGOBLIG, PGFECVAL, PGFECOPER, PGFECVTO, PGFECENT, PGMONEDA, PGTIPO, PGSUBTIPO, PGSIGNO, PGTOTAL, PGCAPITAL, PGINTCOR,
PGIMPUEST, PGGASCOB,PGCOMPROBANTE, PGNOTA, PGIDPAGO, PGPERFIL, PGNMORA, PGHISTORICO, PGFECSIS) VALUES (
{cFormat.StToBD(Pago.PGOBLIG)}, {cFormat.StToBD(Pago.PGFECVAL)},{cFormat.StToBD(Pago.PGFECOPER)}, {cFormat.StToBD(Pago.PGFECVTO)}, {cFormat.StToBD(Pago.PGFECENT)}, 
{cFormat.StToBD(Pago.PGMONEDA)}, {cFormat.StToBD(Pago.PGTIPO)}, {cFormat.StToBD(Pago.PGSUBTIPO)}, {cFormat.StToBD(Pago.PGSIGNO)}, {cFormat.NumToBD(Pago.PGTOTAL.ToString())}, 
{cFormat.NumToBD(Pago.PGCAPITAL.ToString())}, {cFormat.NumToBD(Pago.PGINTCOR.ToString())},{cFormat.NumToBD(Pago.PGIMPUEST.ToString())}, 
{cFormat.NumToBD(Pago.PGGASCOB.ToString())}, {cFormat.StToBD(Pago.PGCOMPROBANTE)}, {cFormat.StToBD(Pago.PGNOTA)}, {cFormat.StToBD(Pago.PGIDPAGO)},
{cFormat.NumToBD(Pago.PGPERFIL.ToString())}, {cFormat.NumToBD(Pago.PGNMORA.ToString())}, {cFormat.StToBD(Pago.PGHISTORICO)}, {cFormat.StToBD(Pago.PGFECSIS)}
)";
            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertaPagoBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSRPAGO", "Error al insertar el pago " + Pago.PGOBLIG);
                return false;
            }
        }

        /// <summary>
        /// Insertamos de forma masiva todos los pagos del dia de Hoy
        /// </summary>
        internal bool InsertPagos()
        {
            bool res = false;

            string sSql = $@"insert into RPAGOS (PGOBLIG, PGFECVAL, PGFECENT, PGFECSIS, PGPERFIL, PGNMORA, PGHISTORICO, PGTIPO, PGNOTA, PGMONEDA,  
PGFECOPER, PGGASCOB, PGCOMPROBANTE, PGFECVTO, PGSIGNO, PGTOTAL, PGCAPITAL, PGINTCOR, PGIMPUEST, PGSUBTIPO, PGIDPAGO ) 
select PGOBLIG, PGFECVAL, PGFECENT, PGFECPROC PGFECSIS, LOPERFIL PGPERFIL, LONMORA PGNMORA, 0 as PGHISTORICO, PGTIPO, PGNOTA, PGMONEDA,  
PGFECOPER, PGGASCOB, PGCOMPROBANTE, PGFECVTO, PGSIGNO, PGTOTAL, PGCAPITAL, PGINTCOR, PGIMPUEST, PGSUBTIPO, PGIDPAGO 
from in_pago
JOIN IN_RCYO on OCOBLIG = PGOBLIG
LEFT JOIN RLOTE ON LOCOD = OCRAIZ
where PGFECPROC = '{cGlobales.Hoy}' ";

            int nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Movimientos tratados: {nRet}");
                res = true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertPagos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSPAGO", "Error al insertar Movimientos ");
                res = false;
            }

            return res;
        }

        /// <summary>
        /// Actualiza el estado de los convenios cumplidos o cancelados
        /// </summary>
        internal bool ActualizaAcuerdos()
        {
            String sSql;

            //Marcamos como cumplidos los convenios que no tienen saldos pendientes y su estado era pendiente
            sSql = @"update rconvp set coestado = '3' where coestado = '1' and not exists 
            (select 1 from RCUOTASCONVP where cuconv = coconv and CUSALDOPENDCUOTA > 0)";

            int res = conn.EjecutarScalar(sSql);

            this.Errores = conn.Errores;

            if (Errores.Cantidad() != 0)
            {
                this.Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizarAcuerdos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(this.Errores, "ActualizarAcuerdos", "Error al actualzar Acuerdos " + conn.Errores.Item(0).sDescripcion);
                return false;
            }
            else
            {
                //Marcamos su estado como cancelado por al no estar pagados y haber vencido su fecha máxima de vencimiento
                sSql = @"update rconvp set coestado = '2' where coestado = '1' and exists
                (select 1 from RCUOTASCONVP where cuconv = coconv group by cuconv having max(CUFECVENC) < {0})";

                res = conn.EjecutarScalar(string.Format(sSql, cFormat.StToBD(cGlobales.Hoy)));

                this.Errores = conn.Errores;

                if (Errores.Cantidad() != 0)
                {
                    this.Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizarAcuerdos", Const.SEVERIDAD_Alta);
                    cIncidencia.Generar(this.Errores, "ActualizarAcuerdos", "Error al actualzar Acuerdos " + conn.Errores.Item(0).sDescripcion);
                    return false;
                }
            }

            return true;
        }
    }
}
