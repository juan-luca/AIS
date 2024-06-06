using BusComun;
using Comun;
using System;
using System.Data;

namespace BusInchost
{
    public class TyObligacion
    {
        public TyObligacion()
        {
            RHisto = new TyHistorico();
        }

        public string OGFECPROC;
        public string OGCOD;
        public string OGTIPO;
        public string OGEMPRESA;
        public string OGOFIC;
        public string OGMONEDA;
        public string OGFECENT;
        public string OGFECMOR;
        public string OGFECSALMORA;
        public string OGFECSIS;
        public string OGNUMPROD;
        public string OGNUMCTA;
        public string OGCANAL;
        public string OGBANCA;
        public string OGIVA;
        public string OGLINEA;
        public double OGMONTORIG;
        public double OGMONTTRANS;
        public string OGGRUPOAFINIDAD;
        public string OGDESCGRUPOAFINIDAD;
        public double OGTASAAP;
        public string OGCAMPANA;
        public string OGMOTSALIDA;
        public string OGNOTAS;
        public string OGTIPOPAQ;
        public string OGNUMPAQ;
        public string OGESTADOPAQ;
        public double OGDEUDPAQ;
        public string OGFECPAQ;
        public string OGREGION;
        public string OGFECMOD;

        public string CLCOD;
        public TyHistorico RHisto;

    }

    class ObligacionesDalc : cBase
    {
        public ObligacionesDalc(cConexion pconn)
        {
            conn = pconn;
        }
        
        internal AISDataReader AbrirCursorObligaciones()
        {

            AISDataReader Dr = new AISDataReader();

            string sSql = $@" SELECT A.OGCOD, A.OGTIPO, A.OGEMPRESA, A.OGOFIC, A.OGMONEDA, A.OGFECENT, A.OGFECMOR, A.OGFECSALMORA, A.OGFECSIS, A.OGNUMPROD, 
A.OGNUMCTA, A.OGCANAL, A.OGBANCA, A.OGIVA, A.OGLINEA, A.OGMONTORIG, A.OGMONTTRANS, A.OGGRUPOAFINIDAD, A.OGDESCGRUPOAFINIDAD, 
A.OGTASAAP, A.OGCAMPANA, A.OGMOTSALIDA, A.OGNOTAS, A.OGTIPOPAQ, A.OGNUMPAQ, A.OGESTADOPAQ, A.OGDEUDPAQ, A.OGFECPAQ,
  HIOBLIG, HIFECENT, HIFECSAL, HINUMCP, HINUMCPOK, 
  HIVECES,HIMEDDIAS, HIMAXDIAS, 
  COALESCE(B.OCRAIZ,'' ) ORDENCLIE
  FROM IN_OBLG A
  LEFT JOIN ROBCL B ON B.OCOBLIG = A.OGCOD AND B.OCFIGURA='T01'
  LEFT JOIN RHISTO ON HIOBLIG = A.OGCOD
  WHERE A.OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)}
ORDER BY ORDENCLIE ";

            //UNION
            //SELECT A.OGCOD, A.OGTIPO, A.OGEMPRESA, A.OGOFIC, A.OGMONEDA, A.OGFECENT, A.OGFECMOR, A.OGFECSALMORA, A.OGFECSIS, A.OGNUMPROD, 
            //A.OGNUMCTA, A.OGCANAL, A.OGBANCA, A.OGIVA, A.OGLINEA, A.OGMONTORIG, A.OGMONTTRANS, A.OGGRUPOAFINIDAD, A.OGDESCGRUPOAFINIDAD, 
            //A.OGTASAAP, A.OGCAMPANA, A.OGMOTSALIDA, A.OGNOTAS, A.OGTIPOPAQ, A.OGNUMPAQ, A.OGESTADOPAQ, A.OGDEUDPAQ, A.OGFECPAQ,
            //  HIOBLIG, HIFECENT, HIFECSAL, HINUMCP, HINUMCPOK, 
            //  HIVECES,HIMEDDIAS, HIMAXDIAS, 
            //  COALESCE(B.OGLOTE,'' ) ORDENCLIE
            //  FROM IN_OBLG A
            //  LEFT JOIN ROBLG B ON B.OGCOD = A.OGCOD 
            //  LEFT JOIN RHISTO ON HIOBLIG = A.OGCOD
            //  WHERE A.OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)}

            #if DEBUG
            sSql += " FETCH FIRST 10 rows Only";
            #endif

            Dr = conn.EjecutarDataReader(sSql);

            Errores = conn.Errores;
            return Dr;
        }

        internal bool FechObligacion(AISDataReader DrC, ref TyObligacion Obligacion)
        {
            try
            {
                Obligacion = new TyObligacion();
                if (DrC.Read())
                {
                    Obligacion.OGCOD = DrC["OGCOD"].ToString();
                    Obligacion.OGTIPO = DrC["OGTIPO"].ToString();
                    Obligacion.OGEMPRESA = DrC["OGEMPRESA"].ToString();
                    Obligacion.OGOFIC = DrC["OGOFIC"].ToString().Length > 1 ? DrC["OGOFIC"].ToString().Substring(1, DrC["OGOFIC"].ToString().Length - 1) : DrC["OGOFIC"].ToString();
                    Obligacion.OGREGION = Obligacion.OGOFIC;
                    Obligacion.OGMONEDA = DrC["OGMONEDA"].ToString().PadLeft(9, '0');
                    Obligacion.OGFECENT = DrC["OGFECENT"].ToString();
                    Obligacion.OGFECMOR = DrC["OGFECMOR"].ToString();
                    Obligacion.OGFECSALMORA = DrC["OGFECSALMORA"].ToString();
                    Obligacion.OGFECSIS = DrC["OGFECSIS"].ToString();
                    Obligacion.OGNUMPROD = DrC["OGNUMPROD"].ToString();
                    Obligacion.OGNUMCTA = DrC["OGNUMCTA"].ToString();
                    Obligacion.OGCANAL = DrC["OGCANAL"].ToString();
                    Obligacion.OGBANCA = DrC["OGBANCA"].ToString();
                    Obligacion.OGIVA = DrC["OGIVA"].ToString();
                    Obligacion.OGLINEA = DrC["OGLINEA"].ToString();
                    Obligacion.OGMONTORIG = cFormat.NumBDToPC(DrC["OGMONTORIG"].ToString());
                    Obligacion.OGMONTTRANS = cFormat.NumBDToPC(DrC["OGMONTTRANS"].ToString());
                    Obligacion.OGGRUPOAFINIDAD = DrC["OGGRUPOAFINIDAD"].ToString();
                    Obligacion.OGDESCGRUPOAFINIDAD = DrC["OGDESCGRUPOAFINIDAD"].ToString();
                    Obligacion.OGTASAAP = cFormat.NumBDToPC(DrC["OGTASAAP"].ToString());
                    Obligacion.OGCAMPANA = DrC["OGCAMPANA"].ToString();
                    Obligacion.OGMOTSALIDA = DrC["OGMOTSALIDA"].ToString();
                    Obligacion.OGNOTAS = DrC["OGNOTAS"].ToString();
                    Obligacion.OGTIPOPAQ = DrC["OGTIPOPAQ"].ToString();
                    Obligacion.OGNUMPAQ = DrC["OGNUMPAQ"].ToString();
                    Obligacion.OGESTADOPAQ = DrC["OGESTADOPAQ"].ToString();
                    Obligacion.OGDEUDPAQ = cFormat.NumBDToPC(DrC["OGDEUDPAQ"].ToString());
                    Obligacion.OGFECPAQ = DrC["OGFECPAQ"].ToString();
                    Obligacion.OGFECMOD = cGlobales.Hoy;

                    Obligacion.CLCOD = DrC["ORDENCLIE"].ToString().Trim();

                    Obligacion.RHisto.HIOBLIG = DrC["HIOBLIG"].ToString().Trim();
                    Obligacion.RHisto.HIFECENT = DrC["HIFECENT"].ToString().Trim();
                    Obligacion.RHisto.HIFECSAL = DrC["HIFECSAL"].ToString().Trim();
                    Obligacion.RHisto.HINUMCP = Convert.ToInt64(cFormat.NumBDToPC(DrC["HINUMCP"].ToString().Trim()));
                    Obligacion.RHisto.HINUMCPOK = Convert.ToInt64(cFormat.NumBDToPC(DrC["HINUMCPOK"].ToString().Trim()));
                    Obligacion.RHisto.HIVECES = Convert.ToInt64(cFormat.NumBDToPC(DrC["HIVECES"].ToString().Trim()));
                    Obligacion.RHisto.HIMEDDIAS = Convert.ToInt64(cFormat.NumBDToPC(DrC["HIMEDDIAS"].ToString().Trim()));
                    Obligacion.RHisto.HIMAXDIAS = Convert.ToInt64(cFormat.NumBDToPC(DrC["HIMAXDIAS"].ToString().Trim()));

                    return true;
                }
                else
                    return false;

            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechObligacion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHOBLG", "Error al recuperar Obligacion");
                return false;
            }
        }

        internal string GetCodigoCliente(string sOblig)
        {
            string codCliente = "";

            try
            {
                DataSet Ds = new DataSet();

                string sSql = $@" SELECT OCRAIZ FROM ROBCL 
WHERE OCOBLIG= {cFormat.StToBD(sOblig)}
AND OCFIGURA='T01' ";

                Ds = conn.EjecutarQuery(sSql, "ROBCL");

                if (conn.Errores.Cantidad() == 0)
                {
                    if (Ds.Tables["ROBCL"].Rows.Count == 1)
                        codCliente = Ds.Tables["ROBCL"].Rows[0]["OCRAIZ"].ToString();
                }
                else
                {
                    Errores = conn.Errores;
                    cIncidencia.Generar(Errores, "SELCLIE", "Error al recuperar el cliente titular para la obligacion: " + sOblig);
                }
            }
            catch
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, "", "GetCodigoCliente", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "SELCLIE", "Error al recuperar el cliente titular para la obligacion: " + sOblig);
            }

            return codCliente;
        }

        internal bool InsertaObligacion(TyObligacion Obligacion)
        {
            String sSql;
            int nRet;

            sSql = $@" INSERT INTO ROBLG (OGCOD, OGTIPO, OGEMPRESA, OGOFIC, OGMONEDA, OGFECENT, OGFECMOR, OGFECSALMORA, OGFECSIS, OGNUMPROD, OGNUMCTA, OGCANAL, 
OGBANCA, OGIVA, OGLINEA, OGMONTORIG, OGMONTTRANS, OGGRUPOAFINIDAD, OGDESCGRUPOAFINIDAD, OGTASAAP, OGCAMPANA, OGMOTSALIDA, OGNOTAS, OGTIPOPAQ, 
OGNUMPAQ, OGESTADOPAQ, OGDEUDPAQ, OGFECPAQ, OGREGION, OGFECMOD)
VALUES ({cFormat.StToBD(Obligacion.OGCOD)}, {cFormat.StToBD(Obligacion.OGTIPO)}, {cFormat.StToBD(Obligacion.OGEMPRESA)}, 
{cFormat.StToBD(Obligacion.OGOFIC)}, {cFormat.StToBD(Obligacion.OGMONEDA)}, {cFormat.StToBD(Obligacion.OGFECENT)}, {cFormat.StToBD(Obligacion.OGFECMOR)},
{cFormat.StToBD(Obligacion.OGFECSALMORA)}, {cFormat.StToBD(Obligacion.OGFECSIS)}, {cFormat.StToBD(Obligacion.OGNUMPROD)}, {cFormat.StToBD(Obligacion.OGNUMCTA)}, 
{cFormat.StToBD(Obligacion.OGCANAL)}, {cFormat.StToBD(Obligacion.OGBANCA)}, {cFormat.StToBD(Obligacion.OGIVA)}, {cFormat.StToBD(Obligacion.OGLINEA)}, 
{cFormat.NumToBD(Obligacion.OGMONTORIG.ToString())}, {cFormat.NumToBD(Obligacion.OGMONTTRANS.ToString())}, {cFormat.StToBD(Obligacion.OGGRUPOAFINIDAD)}, 
{cFormat.StToBD(Obligacion.OGDESCGRUPOAFINIDAD)}, {cFormat.NumToBD(Obligacion.OGTASAAP.ToString())}, {cFormat.StToBD(Obligacion.OGCAMPANA)}, 
{cFormat.StToBD(Obligacion.OGMOTSALIDA)}, {cFormat.StToBD(Obligacion.OGNOTAS)}, {cFormat.StToBD(Obligacion.OGTIPOPAQ)}, {cFormat.StToBD(Obligacion.OGNUMPAQ)}, 
{cFormat.StToBD(Obligacion.OGESTADOPAQ)}, {cFormat.NumToBD(Obligacion.OGDEUDPAQ.ToString())}, {cFormat.StToBD(Obligacion.OGFECPAQ)}, 
{cFormat.StToBD(Obligacion.OGREGION)}, {cFormat.StToBD(Obligacion.OGFECMOD)} ) ";

            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertaObligacion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSOBLG", "Error al insertar Obligacion. " + Obligacion.OGCOD);
                return false;
            }
        }

        internal bool CargaParcialObligModBD(TyObligacion BDObligacion)
        {
            try
            {
                DataSet Ds = new DataSet();

                string sSql = $@" SELECT OGFECMOR, OGFECENT, OGLOTE 
FROM ROBLG 
WHERE OGCOD = {cFormat.StToBD(BDObligacion.OGCOD)} ";

                Ds = conn.EjecutarQuery(sSql, "ROBLG");

                if (conn.Errores.Cantidad() == 0)
                {
                    if (Ds.Tables["ROBLG"].Rows.Count == 1)
                    {
                        BDObligacion.OGFECMOR = Ds.Tables["ROBLG"].Rows[0]["OGFECMOR"].ToString().Trim();
                        BDObligacion.OGFECENT = Ds.Tables["ROBLG"].Rows[0]["OGFECENT"].ToString().Trim();
                        BDObligacion.CLCOD = Ds.Tables["ROBLG"].Rows[0]["OGLOTE"].ToString().Trim();
                        return true;
                    }
                    else
                        return false;
                }
                else
                    return false;
            }
            catch
            {
                return false;
            }
        }

        internal bool ActualizaObligacion(TyObligacion Obligacion)
        {
            int nRet;

            string sSql = $@" UPDATE ROBLG SET 
    OGTIPO = {cFormat.StToBD(Obligacion.OGTIPO)},
    OGEMPRESA = {cFormat.StToBD(Obligacion.OGEMPRESA)},
    OGOFIC = {cFormat.StToBD(Obligacion.OGOFIC)},
    OGMONEDA = {cFormat.StToBD(Obligacion.OGMONEDA)},
    OGFECENT = {cFormat.StToBD(Obligacion.OGFECENT)},
    OGFECMOR = {cFormat.StToBD(Obligacion.OGFECMOR)}, 
    OGFECSALMORA = {cFormat.StToBD(Obligacion.OGFECSALMORA)}, 
    OGFECSIS = {cFormat.StToBD(Obligacion.OGFECSIS)}, 
    OGNUMPROD = {cFormat.StToBD(Obligacion.OGNUMPROD)}, 
    OGNUMCTA = {cFormat.StToBD(Obligacion.OGNUMCTA)}, 
    OGCANAL = {cFormat.StToBD(Obligacion.OGCANAL)},
    OGBANCA = {cFormat.StToBD(Obligacion.OGBANCA)},
    OGIVA = {cFormat.StToBD(Obligacion.OGIVA)},
    OGLINEA = {cFormat.StToBD(Obligacion.OGLINEA)},
    OGMONTORIG = {cFormat.NumToBD(Obligacion.OGMONTORIG.ToString())}, 
    OGMONTTRANS = {cFormat.NumToBD(Obligacion.OGMONTTRANS.ToString())}, 
    OGGRUPOAFINIDAD = {cFormat.StToBD(Obligacion.OGGRUPOAFINIDAD)}, 
    OGDESCGRUPOAFINIDAD = {cFormat.StToBD(Obligacion.OGDESCGRUPOAFINIDAD)},
    OGTASAAP = {cFormat.NumToBD(Obligacion.OGTASAAP.ToString())}, 
    OGCAMPANA = {cFormat.StToBD(Obligacion.OGCAMPANA)}, 
    OGMOTSALIDA = {cFormat.StToBD(Obligacion.OGMOTSALIDA)},
    OGNOTAS = {cFormat.StToBD(Obligacion.OGNOTAS)}, 
    OGTIPOPAQ = {cFormat.StToBD(Obligacion.OGTIPOPAQ)}, 
    OGNUMPAQ = {cFormat.StToBD(Obligacion.OGNUMPAQ)}, 
    OGESTADOPAQ = {cFormat.StToBD(Obligacion.OGESTADOPAQ)}, 
    OGDEUDPAQ = {cFormat.NumToBD(Obligacion.OGDEUDPAQ.ToString())}, 
    OGFECPAQ = {cFormat.StToBD(Obligacion.OGFECPAQ)},
    OGREGION = {cFormat.StToBD(Obligacion.OGREGION)},
    OGFECMOD = {cFormat.StToBD(Obligacion.OGFECMOD)}
WHERE OGCOD = {cFormat.StToBD(Obligacion.OGCOD)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizaObligacion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDROBLG", "Obligacion: " + Obligacion.OGCOD);
                return false;
            }
        }

        internal bool ActualizaObligacionTipoConvP(TyObligacion Obligacion)
        {
            String sSql;
            int nRet;

            sSql = $@" UPDATE ROBLG SET 
            OGFECMOR  = {cFormat.StToBD(Obligacion.OGFECMOR)},
            OGFECENT  = {cFormat.StToBD(Obligacion.OGFECENT)},
            OGREGION  = {cFormat.StToBD(Obligacion.OGREGION)}
            WHERE OGCOD = {cFormat.StToBD(Obligacion.OGCOD)}";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizaObligacionTipoConvP", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDROBLGCONVP", "Obligacion: " + Obligacion.OGCOD);
                return false;
            }
        }

        internal DataSet CargarObligacionesDelLote(string sLote)
        {
            String sSql;
            DataSet Dr = new DataSet();

            sSql = "SELECT OGCOD, TBCODE, OGREGION, OGOFIC, OGPASIVO, ";
            sSql += " OGTIPPAS, OGFECLEG, OGFECVEN,  OGFECENT, ";
            sSql += " OGFECMOR, DESALCAP, DESALTOT, DEDIAS,   DECAPITAL, ";
            sSql += " DEMORATOT, DEPAGMIN, OGLOTE, OGLIDER, OGFECCASTIGO, ";
            sSql += " OGMONCASTIGO, OGFECJUD, OGACTIVJUD,OGFECULTPAGO ";
            sSql += " FROM ROBLG, RDEUDA, RTABL, RPRODUCTOS ";
            sSql += " WHERE OGLOTE = " + cFormat.StToBD(sLote) + " ";
            sSql += "   AND DEOBLIG = OGCOD ";
            sSql += "   AND PDEMPRESA = OGEMPRESA ";
            sSql += "   AND PDCOD = OGTIPO ";
            sSql += "   AND TBNUME='5' ";
            sSql += "   AND TBCODE = PDTIPOPROD ";
            sSql += " ORDER BY OGLIDER DESC, DEDIAS ";

            Dr = conn.EjecutarQuery(sSql, "TABLAOBLG");

            if (conn.Errores.Cantidad() == 0)
                return Dr;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarObligacionesDelLote", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "SELOBGLOTE", "Error al seleccionar Obligacion del lote: " + sLote);
                return Dr;
            }
        }

        /// <summary>
        /// Crea las gestiones FADT para las obligaciones que no se hayan actualizado en los últimos 5 días y, que no tengna gestión FADT en ese periodo 
        /// y una gestión RANA posterior a la fecha de modicación de la obligación.
        /// </summary>
        internal bool InsertarGestionesFADT()
        {
            bool retorno = false;

            string sSql = $@"insert into RBGES (BGLOTE, BGFECHA, BGHORA, BGTARREA, BGGESCLIE, BGIDGESTION, BGOBLIG, BGFIGURA, BGNMORANT, BGNMORNEW, 
            BGESTESCENANT, BGESTESCENNEW, BGOWNANT, BGOWNNEW)
            select OGLOTE, {cFormat.StToBD(cGlobales.Hoy)}, {cFormat.StToBD(cGlobales.HoraMotor)},{cFormat.StToBD(Const.TAR_FALTA_ACTUALIZACION_DATOS)}, '0', 
            {cFormat.StToBD(cGlobales.Hoy)} || {cFormat.StToBD(cGlobales.HoraMotor)} || {cFormat.StToBD(Const.TAR_FALTA_ACTUALIZACION_DATOS)}, OGCOD,'NP',
            LONMORA, LONMORA, LOTURNO, LOTURNO, LOPERFIL, LOPERFIL
            from roblg 
            LEFT JOIN RLOTE ON LOCOD = OGLOTE
            where OGFECMOD  < {cFormat.StToBD(cGlobales.Hoy)} AND OGLOTE is not null
            and not exists (select 1 from rbges 
                            WHERE 
                            BGOBLIG = OGCOD AND  
                            BGFECHA >= TO_CHAR(TO_DATE({cFormat.StToBD(cGlobales.Hoy)}, 'YYYYMMDD')-5, 'YYYYMMDD') AND
                            BGFECHA <= {cFormat.StToBD(cGlobales.Hoy)} AND 
                            BGTARREA = {cFormat.StToBD(Const.TAR_FALTA_ACTUALIZACION_DATOS)} )
            AND NOT EXISTS (select 1 from rbges 
                    where BGOBLIG = OGCOD AND 
                    BGTARREA = {cFormat.StToBD(Const.TAR_REGULARIZACION_AUTOMATICA)}  AND
                    BGFECHA > OGFECMOD                    
                    ) ";

            int res = conn.EjecutarQuery(sSql);

            Errores = conn.Errores;

            if (Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizaObligacion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "InsertarGestionesFADT", "Fecha: " + cGlobales.Hoy);
            }
            else
                retorno = true;

            return retorno;
        }

        /// <summary>
        /// Borramos las gestiones FADT de las obligaciones que se han actualziado y no se habían actualido en los últimos 5 días
        /// </summary>
        internal bool BorrarGestionesFADT()
        {
            bool retorno = false;

            string sSql = $@"delete from RBGES 
where 
BGOBLIG IN (select OGCOD 
          from roblg 
          where OGFECMOD  = {cFormat.StToBD(cGlobales.Hoy)}) AND
BGFECHA >= TO_CHAR(TO_DATE({cFormat.StToBD(cGlobales.Hoy)}, 'YYYYMMDD')-5, 'YYYYMMDD') AND
    BGFECHA <= {cFormat.StToBD(cGlobales.Hoy)} AND 
    BGTARREA = {cFormat.StToBD(Const.TAR_FALTA_ACTUALIZACION_DATOS)} ";

            int res = conn.EjecutarQuery(sSql);

            Errores = conn.Errores;

            if (Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizaObligacion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "BorrarGestionesFADT", "Fecha: " + cGlobales.Hoy);
            }
            else
                retorno = true;

            return retorno;
        }

        /// <summary>
        /// Inicializamos la deuda a 0 para las obligaciones a dar de baja
        /// </summary>
        internal bool InicializarDeuda()
        {
            bool retorno = false;

            string sSql = $@" merge into RDEUDA de
using (
        select OGCOD
from roblg
where OGFECMOD < TO_CHAR(TO_DATE({cFormat.StToBD(cGlobales.Hoy)}, 'YYYYMMDD') - 5, 'YYYYMMDD')
    AND OGLOTE is not null
and not exists(select 1 from rbges
   WHERE
   BGOBLIG = OGCOD AND
    BGFECHA >= OGFECMOD AND
    BGTARREA = {cFormat.StToBD(Const.TAR_REGULARIZACION_AUTOMATICA)})
) og on(de.DEOBLIG = OG.OGCOD)
when matched then update
set de.DEDIAS = 0,
    de.DESALTOT = 0, 
    de.DESALCAP = 0, 
    de.DEMORATOT = 0,
    de.DECAPITAL = 0, 
    de.DEINTCOR = 0, 
    de.DEIMPUEST = 0, 
    de.DESALTOTUSD = 0,
    de.DEMORATOTUSD = 0, 
    de.DESALCAPUSD = 0 ";

            int res = conn.EjecutarQuery(sSql);

            Errores = conn.Errores;

            if (Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizaObligacion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "BorrarGestionesFADT", "Fecha: " + cGlobales.Hoy);
            }
            else
                retorno = true;

            return retorno;
        }

        /// <summary>
        /// Se inicializa la Fecha de entrada en Mora de la obligación
        /// </summary>
        internal bool InicializarFechaMora()
        {
            bool retorno = false;

            string sSql = $@" update roblg set OGFECMOR = null 
where OGFECMOD  < TO_CHAR(TO_DATE({cFormat.StToBD(cGlobales.Hoy)}, 'YYYYMMDD')-5, 'YYYYMMDD') 
   AND OGLOTE is not null
   AND not exists (select 1 from rbges
   WHERE
   BGOBLIG = OGCOD AND
    BGFECHA > OGFECMOD AND
    BGTARREA = {cFormat.StToBD(Const.TAR_REGULARIZACION_AUTOMATICA)}) ";

            int res = conn.EjecutarQuery(sSql);

            Errores = conn.Errores;

            if (Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizaObligacion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "InicializarFechaMora", "Fecha: " + cGlobales.Hoy);
            }
            else
                retorno = true;

            return retorno;
        }

        /// <summary>
        /// Insertamos gestiones RANA para las obligaciones que lleven más de 5 días sin actualizarse
        /// </summary>
        internal bool InsertarGestionesRANA()
        {
            bool retorno = false;

            string sSql = $@"insert into RBGES (BGLOTE, BGFECHA, BGHORA, BGTARREA, BGGESCLIE, BGIDGESTION, BGOBLIG, BGFIGURA,
            BGNMORANT, BGNMORNEW, BGESTESCENANT, BGESTESCENNEW, BGOWNANT, BGOWNNEW)
            select OGLOTE, {cFormat.StToBD(cGlobales.Hoy)}, {cFormat.StToBD(cGlobales.HoraMotor)}, {cFormat.StToBD(Const.TAR_REGULARIZACION_AUTOMATICA)}, '0', 
            {cFormat.StToBD(cGlobales.Hoy)} || {cFormat.StToBD(cGlobales.HoraMotor)} || {cFormat.StToBD(Const.TAR_REGULARIZACION_AUTOMATICA)}, OGCOD,'NP',
            LONMORA, LONMORA, LOTURNO, LOTURNO, LOPERFIL, LOPERFIL
            from roblg
            LEFT JOIN RLOTE ON LOCOD = OGLOTE
            where OGFECMOD  < TO_CHAR(TO_DATE({cFormat.StToBD(cGlobales.Hoy)}, 'YYYYMMDD')-5, 'YYYYMMDD') 
            AND OGLOTE is not null
            and not exists (select 1 from rbges 
                WHERE 
                BGOBLIG = OGCOD AND  
                BGFECHA >= OGFECMOD AND
                BGTARREA = {cFormat.StToBD(Const.TAR_REGULARIZACION_AUTOMATICA)} ) ";

            int res = conn.EjecutarQuery(sSql);

            Errores = conn.Errores;

            if (Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ActualizaObligacion", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "InsertarGestionesRANA", "Fecha: " + cGlobales.Hoy);
            }
            else
                retorno = true;

            return retorno;
        }

        #region Atributos dela Obligacion

        internal bool VaciarAtributos()
        {
            int nRet;
            string sSql = $"DELETE FROM RATROBLG P WHERE EXISTS (SELECT 1 FROM IN_ATR_OB TIN  WHERE TIN.OGFECPROC2 = {cFormat.StToBD(cGlobales.Hoy)} AND P.OGACOD = TIN.OGACOD )";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarAtributos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELAtrOblg", "Error al vaciar RATROBLG");
                return false;
            }
        }

        internal bool CargarAtributos()
        {
            int nRet;
            string sSql = $@"INSERT INTO RATROBLG (OGACOD, OGACARTERA, OGAFECHA1, OGAFECHA2, OGAFECHA3, OGAFECHA4, OGAFECHA5, OGAFECHA6, OGAFECHA7, 
OGAFECHA8, OGAFECHA9, OGAFECHA10, OGATEXTO1, OGATEXTO2, OGATEXTO3, OGATEXTO4, OGATEXTO5, OGATEXTO6, OGATEXTO7, OGATEXTO8, OGATEXTO9, OGATEXTO10, 
OGAMONTO1, OGAMONTO2, OGAMONTO3, OGAMONTO4, OGAMONTO5, OGAMONTO6, OGAMONTO7, OGAMONTO8, OGAMONTO9, OGAMONTO10, OGACOEF1, OGACOEF2, OGACOEF3, 
OGACOEF4, OGACOEF5, OGACOEF6, OGACOEF7, OGACOEF8, OGACOEF9, OGACOEF10, OGAOBS)
SELECT OGACOD, OGACARTERA, OGAFECHA1, OGAFECHA2, OGAFECHA3, OGAFECHA4, OGAFECHA5, OGAFECHA6, OGAFECHA7, OGAFECHA8, OGAFECHA9, OGAFECHA10, OGATEXTO1, 
OGATEXTO2, OGATEXTO3, OGATEXTO4, OGATEXTO5, OGATEXTO6, OGATEXTO7, OGATEXTO8, OGATEXTO9, OGATEXTO10, OGAMONTO1, OGAMONTO2, OGAMONTO3, OGAMONTO4, 
OGAMONTO5, OGAMONTO6, OGAMONTO7, OGAMONTO8, OGAMONTO9, OGAMONTO10, OGACOEF1, OGACOEF2, OGACOEF3, OGACOEF4, OGACOEF5, OGACOEF6, OGACOEF7, OGACOEF8, 
OGACOEF9, OGACOEF10, OGAOBS
FROM IN_ATR_OB
WHERE OGFECPROC2 = {cFormat.StToBD(cGlobales.Hoy)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Atributos Obligaciones tratados: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarDocumentos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSAtrOblg", "Error al insertar RATROBLG");
                return false;
            }
        }

        #endregion

        #region Alta de Obligaciones masivas

        /// <summary>
        /// Copiamos los datos que no podemos perder de la tabla productiva a la tabla IN para no perderlos al hacer insert select de obligaciones
        /// </summary>
        internal bool CopiarDatosProductivos()
        {
            string sSql = $@"merge into IN_OBLG entrada
using (
    select OGCOD, OGLOTE, OGLIDER, OGCONVENIO, OGESTADO, OGFECJUD, OGACTIVJUD, OGFECPASAJEJUD, OGESJUDICIAL, OGREFINANCIACION, OGFECSIS
    FROM ROBLG prod
    WHERE EXISTS (select 1 from IN_OBLG i where i.OGCOD = prod.OGCOD and OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)})
) productiva on(entrada.OGCOD = productiva.OGCOD)
when matched then update
set entrada.OGLOTE = productiva.OGLOTE,
entrada.OGLIDER = productiva.OGLIDER,
entrada.OGCONVENIO = productiva.OGCONVENIO,
entrada.OGESTADO = productiva.OGESTADO,
entrada.OGFECJUD = productiva.OGFECJUD,
entrada.OGACTIVJUD = productiva.OGACTIVJUD,
entrada.OGFECPASAJEJUD = productiva.OGFECPASAJEJUD,
entrada.OGESJUDICIAL = productiva.OGESJUDICIAL,
entrada.OGREFINANCIACION = productiva.OGREFINANCIACION, 
entrada.OGFECSIS = productiva.OGFECSIS
where entrada.OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)} ";

            conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarDocumentos", Const.SEVERIDAD_Alta);
                
                return false;
            }
        }

        /// <summary>
        /// Marcamos las obligaciones que se van a dar de Alta (no existen y nunca han existido en RS)
        /// </summary>
        internal void MarcarAltas()
        {
            string sSql = $@"UPDATE IN_OBLG i
SET OGTIPOOPE = 'A'
WHERE OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)}
AND NOT EXISTS (select 1 from ROBLG p where p.OGCOD = i.OGCOD)
AND NOT EXISTS (select 1 from RHISTO h where h.HIOBLIG = i.OGCOD)";

            conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "MarcarAltas", Const.SEVERIDAD_Alta);
                
            }
        }

        /// <summary>
        /// Marcamos las obligaciones que se van a dar de Modificar (existen en RS)
        /// </summary>
        internal void MarcarModificaciones()
        {
            string sSql = $@"UPDATE IN_OBLG i
SET OGTIPOOPE = 'M'
WHERE OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)}
AND EXISTS (select 1 from ROBLG p where p.OGCOD = i.OGCOD) ";

            conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "MarcarModificaciones", Const.SEVERIDAD_Alta);
                
            }
        }

        /// <summary>
        /// Marcamos las obligaciones como Reentrada (no existen en la tabla productiva pero si en el histórico)
        /// </summary>
        internal void MarcarReentradas()
        {
            string sSql = $@"UPDATE IN_OBLG i
SET OGTIPOOPE = 'R'
WHERE OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)}
AND NOT EXISTS (select 1 from ROBLG p where p.OGCOD = i.OGCOD)
AND EXISTS (select 1 from RHISTO h where h.HIOBLIG = i.OGCOD)";

            conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "MarcarReentradas", Const.SEVERIDAD_Alta);
                
            }
        }

        /// <summary>
        /// Se borran de la tabla productiva las Obligaciones que existen en la IN para la fecha de proceso actual
        /// </summary>
        internal void BorrarObligaciones()
        {
            string sSql = $@"delete from ROBLG o
where EXISTS (select 1 from in_oblg i where i.OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)} and o.ogcod = i.ogcod)";

            conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "BorrarObligaciones", Const.SEVERIDAD_Alta);
                
            }
        }

        /// <summary>
        /// Insertamos masivamente todas las obligaciones a cargar en el día
        /// </summary>
        internal void InsertarObligaciones()
        {
            string sSql = $@"insert into ROBLG (OGCOD, OGTIPO, OGEMPRESA, OGOFIC, OGREGION, OGMONEDA, OGFECENT, OGFECMOR, OGFECSALMORA, OGFECSIS, OGNUMPROD, OGNUMCTA, OGCANAL, 
OGBANCA, OGIVA, OGLINEA, OGMONTORIG, OGMONTTRANS, OGGRUPOAFINIDAD, OGTASAAP, OGCAMPANA, OGMOTSALIDA, OGNOTAS, OGTIPOPAQ, OGNUMPAQ, OGESTADOPAQ, OGDEUDPAQ, OGFECPAQ, OGFECMOD,
OGLOTE, OGLIDER, OGCONVENIO, OGESTADO, OGFECJUD, OGACTIVJUD, OGFECPASAJEJUD, OGESJUDICIAL, OGREFINANCIACION)
select OGCOD, OGTIPO, OGEMPRESA, CASE
         WHEN LENGTH(OGOFIC) > 1 THEN SUBSTR(OGOFIC, 2)
         ELSE OGOFIC
       END AS OGOFIC, 
       OGOFIC as OGREGION,
       OGMONEDA, 
       OGFECENT, OGFECMOR, OGFECSALMORA, OGFECSIS, OGNUMPROD, OGNUMCTA, OGCANAL, OGBANCA, OGIVA, OGLINEA, OGMONTORIG, OGMONTTRANS, OGGRUPOAFINIDAD, OGTASAAP, OGCAMPANA, 
       OGMOTSALIDA, OGNOTAS, OGTIPOPAQ, OGNUMPAQ, OGESTADOPAQ, OGDEUDPAQ, OGFECPAQ, {cFormat.StToBD(cGlobales.Hoy)} as OGFECMOD, 
       OGLOTE, OGLIDER, OGCONVENIO, OGESTADO, OGFECJUD, OGACTIVJUD, OGFECPASAJEJUD, OGESJUDICIAL, OGREFINANCIACION
from IN_OBLG

where OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)} ";

            int res = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertarObligaciones", Const.SEVERIDAD_Alta);
            }
            else
            {
                cIncidencia.Aviso($"Obligaciones tratadas: {res}");
            }
        }

        /// <summary>
        /// Creamos los historicos de las obligaciones que se han dado de alta hoy
        /// </summary>
        internal void CrearHistoricos()
        {
            string sSql = $@"INSERT INTO RHISTO(HIOBLIG, HIFECENT, HIFECSAL, HIDIASINH, HINUMCP, HINUMCPOK, HIVECES, HIMEDDIAS, HIMAXDIAS, HIRAIZ)
                            select OGCOD, null, null,0,0,0,0,0,0, OCRAIZ
                            from IN_OBLG
                            JOIN IN_RCYO on OGCOD = OCOBLIG
                            where OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)} and OGTIPOOPE = 'A' ";

            conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CrearHistoricos", Const.SEVERIDAD_Alta);
                
            }
        }

        internal void CrearGestionesAltaReentrada()
        {
            string sSql = $@"insert into RBGES (BGLOTE, BGFECHA, BGHORA, BGTARREA, BGGESCLIE, BGOBLIG, BGFIGURA, BGIDGESTION,
            BGNMORANT, BGNMORNEW, BGESTESCENANT, BGESTESCENNEW, BGOWNANT, BGOWNNEW)
            select OCRAIZ, {cFormat.StToBD(cGlobales.Hoy)}, {cFormat.StToBD(cGlobales.HoraMotor)}, 
            CASE OGTIPOOPE when 'R' then '{Const.TAR_REENTRADA}' 
                                    else '{Const.TAR_ENTRADA_MORA}' end, 
                '0', OGCOD, 'NP', 
            CASE OGTIPOOPE when 'R' then '{cGlobales.Hoy}{cGlobales.HoraMotor}{Const.TAR_REENTRADA}0' 
                           else '{cGlobales.Hoy}{cGlobales.HoraMotor}{Const.TAR_ENTRADA_MORA}0' end,
            LONMORA, LONMORA, LOTURNO, LOTURNO, LOPERFIL, LOPERFIL
            from IN_OBLG
            join IN_RCYO on OGCOD = OCOBLIG 
            LEFT JOIN RLOTE ON LOCOD = OCRAIZ
            where OGFECPROC = {cFormat.StToBD(cGlobales.Hoy)} AND OGTIPOOPE in ('R','A')";

            conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarDocumentos", Const.SEVERIDAD_Alta);
            }
        }

        #endregion
    }
}
