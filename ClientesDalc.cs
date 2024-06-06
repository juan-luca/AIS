using BusComun;
using Comun;
using System;
using System.Data;

namespace BusInchost
{
    public class TyCliente
    {
        public string CLFECPROC;
        public string CLEMPRESA;
        public string CLCOD;
        public string CLTIPOPER;
        public string CLTIPDOC;
        public string CLNUMDOC;
        public string CLSEGMENTO;
        public string CLAPELLIDO1;
        public string CLNOMBRE;
        public string CLACTECO;
        public string CLSUCCLIE;
        public string CLVIP;
        public string CLFECNAC;
        public string CLNACIONALIDAD;
        public string CLSEXO;
        public string CLESTCIV;
        public string CLENTIDAD;
        public string CLCUIT;
        public string CLEMPLEADO;
        public string CLSITLEGAL;
        public string CLFECSITLEGAL;
        public string CLCIV;
        public string CLCLASE;
        public string CLBANCA;
        public string CLSUBBANCA;
        public string CLDINERS;
        public string CLRESPONSABLE;
        public string CLPROV;
        public string CLCRED;
        public string CLDIA;
        public string CLOBSERV;
        public string CLNOTAS;
        public string CLOFICIAL;
        public string CLRESPCREDITOS;
        public string CLCODSITBANC;
        public string CLDESCSITBANC;
        public string CLTIPOCLIE;
    }

    class ClientesDalc : cBase
    {
        public ClientesDalc(cConexion pconn)
        {
            conn = pconn;
        }

        #region Proceos Masivo

        /// <summary>
        /// Borrado de clientes productos que llegan en la IN
        /// </summary>
        internal bool BorradoClientes()
        {
            string sSql = $@"delete from RCLIE p where exists (select 1 from in_clie i where i.CLFECPROC = '{cGlobales.Hoy}' AND p.CLCOD = i.clcod)";

            int nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(conn.Errores);
                return false;
            }
            else
            {
                return true;
            }
        }

        /// <summary>
        /// Alta masiva de clientes
        /// </summary>
        internal bool AltaClientes()
        {
            string sSql = $@"INSERT INTO RCLIE (
CLEMPRESA, CLCOD, CLTIPOPER, CLTIPDOC, CLNUMDOC, CLSEGMENTO, CLAPELLIDO1, CLNOMBRE, CLACTECO, CLSUCCLIE, CLVIP, CLFECNAC, CLNACIONALIDAD, CLSEXO, 
CLESTCIV, CLENTIDAD, CLCUIT, CLEMPLEADO, CLSITLEGAL, CLFECSITLEGAL, CLCIV, CLCLASE, CLBANCA, CLSUBBANCA, CLDINERS, CLRESPONSABLE, CLPROV, CLCRED, 
CLDIA, CLOBSERV, CLNOTAS, CLOFICIAL, CLRESPCREDITOS, CLCODSITBANC, CLDESCSITBANC, CLTIPOCLIE )
SELECT CLEMPRESA, CLCOD, CLTIPOPER, CLTIPDOC, CLNUMDOC, CLSEGMENTO, CLAPELLIDO1, CLNOMBRE, CLACTECO, 
    CASE 
        WHEN LENGTH(CLSUCCLIE) > 1 THEN SUBSTR(CLSUCCLIE, 2) 
	    ELSE CLSUCCLIE 
    END AS CLSUCCLIE, CLVIP, CLFECNAC, CLNACIONALIDAD, CLSEXO, CLESTCIV, CLENTIDAD, CLCUIT, CLEMPLEADO, CLSITLEGAL, CLFECSITLEGAL, CLCIV, CLCLASE, 
    CLBANCA, CLSUBBANCA, CLDINERS, CLRESPONSABLE, CLPROV, CLCRED, CLDIA, CLOBSERV, CLNOTAS, CLOFICIAL, CLRESPCREDITOS, CLCODSITBANC, CLDESCSITBANC, 
    'V' as CLTIPOCLIE
FROM IN_CLIE
WHERE CLFECPROC = '{cGlobales.Hoy}' AND EXISTS (SELECT 1 FROM ROBCL WHERE OCRAIZ = CLCOD) ";

            int nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                this.Errores.Agregar(conn.Errores);
                return false;
            }
            else
            {
                cIncidencia.Aviso($"Clientes tratados: {nRet} ");
                return true;
            }
        }

        #endregion

        #region Proceso Unitarios

        [Obsolete("Por el momento no hay bajas", true)]
        internal bool BajasDeClientes()
        {
            int nRet;
            String sSql;

            sSql = "UPDATE RCLIE SET CLESTADO ='B' ";
            sSql += " WHERE EXISTS (SELECT B.CLCOD FROM IN_CLIE B ";
            sSql += " 	       WHERE B.CLTIPOOPER='B' AND B.CLCOD= RCLIE.CLCOD ";
            sSql += " 	         AND B.CLFECPROC = " + cFormat.StToBD(cGlobales.Hoy) + ") ";


            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "BajasDeClientes", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "BAJACLIE", "Error marcando clientes a dar de baja");
                return false;
            }
        }

        internal AISDataReader AbrirCursorNuevosClientes()
        {
            AISDataReader Dr = new AISDataReader();

            string sSql = $@"SELECT CLFECPROC, CLEMPRESA, CLCOD, CLTIPOPER, CLTIPDOC, CLNUMDOC, CLSEGMENTO, CLAPELLIDO1, CLNOMBRE, CLACTECO, CLSUCCLIE, 
CLVIP, CLFECNAC, CLNACIONALIDAD, CLSEXO, CLESTCIV, CLENTIDAD, CLCUIT, CLEMPLEADO, CLSITLEGAL, CLFECSITLEGAL, CLCIV, CLCLASE, CLBANCA, CLSUBBANCA, 
CLDINERS, CLRESPONSABLE, CLPROV, CLCRED, CLDIA, CLOBSERV, CLNOTAS, CLOFICIAL, CLRESPCREDITOS, CLCODSITBANC, CLDESCSITBANC, 'V' CLTIPOCLIE
FROM IN_CLIE where CLFECPROC = {cFormat.StToBD(cGlobales.Hoy)}";

#if DEBUG
            sSql += " FETCH FIRST 10 rows Only";
#endif

            Dr = conn.EjecutarDataReader(sSql);

            Errores = conn.Errores;
            return Dr;
        }

        internal bool FechCliente(AISDataReader DrC, ref TyCliente Cliente)
        {
            try
            {
                Cliente = new TyCliente();
                if (DrC.Read())
                {
                    Cliente.CLEMPRESA = DrC["CLEMPRESA"].ToString();
                    Cliente.CLCOD = DrC["CLCOD"].ToString();
                    Cliente.CLTIPOPER = DrC["CLTIPOPER"].ToString();
                    Cliente.CLTIPDOC = DrC["CLTIPDOC"].ToString();
                    Cliente.CLNUMDOC = DrC["CLNUMDOC"].ToString();
                    Cliente.CLSEGMENTO = DrC["CLSEGMENTO"].ToString();
                    Cliente.CLAPELLIDO1 = DrC["CLAPELLIDO1"].ToString();
                    Cliente.CLNOMBRE = DrC["CLNOMBRE"].ToString();
                    Cliente.CLACTECO = DrC["CLACTECO"].ToString();
                    Cliente.CLSUCCLIE = DrC["CLSUCCLIE"].ToString().Length > 1 ? DrC["CLSUCCLIE"].ToString().Substring(1, DrC["CLSUCCLIE"].ToString().Length - 1) : DrC["CLSUCCLIE"].ToString();
                    Cliente.CLVIP = DrC["CLVIP"].ToString();
                    Cliente.CLFECNAC = DrC["CLFECNAC"].ToString();
                    Cliente.CLNACIONALIDAD = DrC["CLNACIONALIDAD"].ToString();
                    Cliente.CLSEXO = DrC["CLSEXO"].ToString();
                    Cliente.CLESTCIV = DrC["CLESTCIV"].ToString();
                    Cliente.CLENTIDAD = DrC["CLENTIDAD"].ToString();
                    Cliente.CLCUIT = DrC["CLCUIT"].ToString();
                    Cliente.CLEMPLEADO = DrC["CLEMPLEADO"].ToString();
                    Cliente.CLSITLEGAL = DrC["CLSITLEGAL"].ToString();
                    Cliente.CLFECSITLEGAL = DrC["CLFECSITLEGAL"].ToString();
                    Cliente.CLCIV = DrC["CLCIV"].ToString();
                    Cliente.CLCLASE = DrC["CLCLASE"].ToString();
                    Cliente.CLBANCA = DrC["CLBANCA"].ToString();
                    Cliente.CLSUBBANCA = DrC["CLSUBBANCA"].ToString();
                    Cliente.CLDINERS = DrC["CLDINERS"].ToString();
                    Cliente.CLRESPONSABLE = DrC["CLRESPONSABLE"].ToString();
                    Cliente.CLPROV = DrC["CLPROV"].ToString();
                    Cliente.CLCRED = DrC["CLCRED"].ToString();
                    Cliente.CLDIA = DrC["CLDIA"].ToString();
                    Cliente.CLOBSERV = DrC["CLOBSERV"].ToString();
                    Cliente.CLNOTAS = DrC["CLNOTAS"].ToString();
                    Cliente.CLOFICIAL = DrC["CLOFICIAL"].ToString();
                    Cliente.CLRESPCREDITOS = DrC["CLRESPCREDITOS"].ToString();
                    Cliente.CLCODSITBANC = DrC["CLCODSITBANC"].ToString();
                    Cliente.CLDESCSITBANC = DrC["CLDESCSITBANC"].ToString();

                    Cliente.CLTIPOCLIE = DrC["CLTIPOCLIE"].ToString().Trim();

                    return true;
                }
                else
                    return false;

            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechCliente", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHCLIE", "Error al recuperar Cliente");
                return false;
            }
        }

        internal bool InsertarCliente(TyCliente Cliente)
        {
            int nRet = 0;

            string sSql = $@"INSERT INTO RCLIE (CLEMPRESA, CLCOD, CLTIPOPER, CLTIPDOC, CLNUMDOC, CLSEGMENTO, CLAPELLIDO1, CLNOMBRE, CLACTECO, CLSUCCLIE, 
CLVIP, CLFECNAC, CLNACIONALIDAD, CLSEXO, CLESTCIV, CLENTIDAD, CLCUIT, CLEMPLEADO, CLSITLEGAL, CLFECSITLEGAL, CLCIV, CLCLASE, CLBANCA, CLSUBBANCA, 
CLDINERS, CLRESPONSABLE, CLPROV, CLCRED, CLDIA, CLOBSERV, CLNOTAS, CLOFICIAL, CLRESPCREDITOS, CLCODSITBANC, CLDESCSITBANC, CLTIPOCLIE) 
VALUES ({cFormat.StToBD(Cliente.CLEMPRESA)}, {cFormat.StToBD(Cliente.CLCOD)}, {cFormat.StToBD(Cliente.CLTIPOPER)}, {cFormat.StToBD(Cliente.CLTIPDOC)},
{cFormat.StToBD(Cliente.CLNUMDOC)}, {cFormat.StToBD(Cliente.CLSEGMENTO)}, {cFormat.StToBD(Cliente.CLAPELLIDO1)}, {cFormat.StToBD(Cliente.CLNOMBRE)},
{cFormat.StToBD(Cliente.CLACTECO)}, {cFormat.StToBD(Cliente.CLSUCCLIE)}, {cFormat.StToBD(Cliente.CLVIP)}, {cFormat.StToBD(Cliente.CLFECNAC)},
{cFormat.StToBD(Cliente.CLNACIONALIDAD)}, {cFormat.StToBD(Cliente.CLSEXO)}, {cFormat.StToBD(Cliente.CLESTCIV)}, {cFormat.StToBD(Cliente.CLENTIDAD)},
{cFormat.StToBD(Cliente.CLCUIT)}, {cFormat.StToBD(Cliente.CLEMPLEADO)}, {cFormat.StToBD(Cliente.CLSITLEGAL)}, {cFormat.StToBD(Cliente.CLFECSITLEGAL)},
{cFormat.StToBD(Cliente.CLCIV)}, {cFormat.StToBD(Cliente.CLCLASE)}, {cFormat.StToBD(Cliente.CLBANCA)}, {cFormat.StToBD(Cliente.CLSUBBANCA)},
{cFormat.StToBD(Cliente.CLDINERS)}, {cFormat.StToBD(Cliente.CLRESPONSABLE)}, {cFormat.StToBD(Cliente.CLPROV)}, {cFormat.StToBD(Cliente.CLCRED)}, 
{cFormat.StToBD(Cliente.CLDIA)}, {cFormat.StToBD(Cliente.CLOBSERV)}, {cFormat.StToBD(Cliente.CLNOTAS)}, {cFormat.StToBD(Cliente.CLOFICIAL)}, 
{cFormat.StToBD(Cliente.CLRESPCREDITOS)}, {cFormat.StToBD(Cliente.CLCODSITBANC)}, {cFormat.StToBD(Cliente.CLDESCSITBANC)}, {cFormat.StToBD(Cliente.CLTIPOCLIE)})";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertarCliente", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSCLIE", "Error al insertar cliente " + Cliente.CLCOD);
                return false;
            }
        }

        internal bool ModificarCliente(TyCliente Cliente)
        {
            String sSql;
            int nRet;

            sSql = $@" UPDATE RCLIE SET 
    CLEMPRESA = {cFormat.StToBD(Cliente.CLEMPRESA)},
    CLTIPOPER = {cFormat.StToBD(Cliente.CLTIPOPER)},
    CLTIPDOC = {cFormat.StToBD(Cliente.CLTIPDOC)},
    CLNUMDOC = {cFormat.StToBD(Cliente.CLNUMDOC)},
    CLSEGMENTO = {cFormat.StToBD(Cliente.CLSEGMENTO)},
    CLAPELLIDO1 = {cFormat.StToBD(Cliente.CLAPELLIDO1)},
    CLNOMBRE = {cFormat.StToBD(Cliente.CLNOMBRE)},
    CLACTECO = {cFormat.StToBD(Cliente.CLACTECO)},
    CLSUCCLIE = {cFormat.StToBD(Cliente.CLSUCCLIE)},
    CLVIP = {cFormat.StToBD(Cliente.CLVIP)},
    CLFECNAC = {cFormat.StToBD(Cliente.CLFECNAC)},
    CLNACIONALIDAD = {cFormat.StToBD(Cliente.CLNACIONALIDAD)},
    CLSEXO = {cFormat.StToBD(Cliente.CLSEXO)},
    CLESTCIV = {cFormat.StToBD(Cliente.CLESTCIV)},
    CLENTIDAD = {cFormat.StToBD(Cliente.CLENTIDAD)},
    CLCUIT = {cFormat.StToBD(Cliente.CLCUIT)},
    CLEMPLEADO = {cFormat.StToBD(Cliente.CLEMPLEADO)},
    CLSITLEGAL = {cFormat.StToBD(Cliente.CLSITLEGAL)},
    CLFECSITLEGAL = {cFormat.StToBD(Cliente.CLFECSITLEGAL)},
    CLCIV = {cFormat.StToBD(Cliente.CLCIV)},
    CLCLASE = {cFormat.StToBD(Cliente.CLCLASE)},
    CLBANCA = {cFormat.StToBD(Cliente.CLBANCA)},
    CLSUBBANCA = {cFormat.StToBD(Cliente.CLSUBBANCA)},
    CLDINERS = {cFormat.StToBD(Cliente.CLDINERS)},
    CLRESPONSABLE = {cFormat.StToBD(Cliente.CLRESPONSABLE)},
    CLPROV = {cFormat.StToBD(Cliente.CLPROV)},
    CLCRED = {cFormat.StToBD(Cliente.CLCRED)},
    CLDIA = {cFormat.StToBD(Cliente.CLDIA)},
    CLOBSERV = {cFormat.StToBD(Cliente.CLOBSERV)},
    CLNOTAS = {cFormat.StToBD(Cliente.CLNOTAS)},
    CLOFICIAL = {cFormat.StToBD(Cliente.CLOFICIAL)},
    CLRESPCREDITOS = {cFormat.StToBD(Cliente.CLRESPCREDITOS)},
    CLCODSITBANC = {cFormat.StToBD(Cliente.CLCODSITBANC)},
    CLDESCSITBANC = {cFormat.StToBD(Cliente.CLDESCSITBANC)},
    CLTIPOCLIE = {cFormat.StToBD(Cliente.CLTIPOCLIE)}
WHERE CLCOD = {cFormat.StToBD(Cliente.CLCOD)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                if (nRet == 0)
                    return false;
                else
                    return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificarCliente", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDCLIE", "Error al modificar cliente " + Cliente.CLCOD);
                return false;
            }

        }

        internal DataSet CargarClientesDelLote(string sLote)
        {
            String sSql;
            DataSet Dr = new DataSet();

            sSql = "SELECT DISTINCT OCFIGURA, CLCOD, CLTIPDOC, CLNUMDOC, CLFECING, ";
            sSql += "  CLACTECO,     CLVIP, CLTIENETELPART, CLTIENETELLABO, ";
            sSql += "  CLTIENETELCELU, CLTIENEDIRPART, CLTIENEDIRLABO, CLTIENEEMAIL, ";
            sSql += "  PVVISTA,  PVNOVISTA, CLTIPOCLIE, SCRECFECEVAL, SCRECGRUPO,  ";
            sSql += "  SCRECPROB, SCCOMFECEVAL, SCCOMGRUPO, SCCOMPROB ";
            sSql += " FROM ROBCL, RCLIE LEFT OUTER JOIN RPASIVO ON (PVRAIZ=CLCOD)  ";
            sSql += "                     LEFT OUTER JOIN RSCORINGS ON (SCRAIZ=CLCOD)  ";
            sSql += " WHERE OCOBLIG IN (SELECT DISTINCT OCOBLIG ";
            sSql += "                    FROM ROBCL WHERE OCRAIZ =" + cFormat.StToBD(sLote) + " AND OCFIGURA='T01') ";
            sSql += "   AND CLCOD = OCRAIZ ";
            sSql += " ORDER BY OCFIGURA ";

            Dr = conn.EjecutarQuery(sSql, "TABLACLIE");

            if (conn.Errores.Cantidad() == 0)
                return Dr;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarClientesDelLote", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "SELCLIELOTE", "Error al seleccionar Clientes del lote: " + sLote);
                return Dr;
            }
        }

        #endregion

        #region Documentos del Cliente

        /// <summary>
        /// Se borran todos los documentos de los distintos clientes que llegan en la interfaz.
        /// </summary>
        internal bool VaciarDocumentos()
        {
            int nRet;
            string sSql = $"DELETE FROM rclidoc P WHERE EXISTS (SELECT 1 FROM in_clidoc  TIN WHERE P.CDCLIE = TIN.CDCLIE AND TIN.CDFECPROC = {cFormat.StToBD(cGlobales.Hoy)})";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarDocumentos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELCliDoc", "Error al vaciar RCLIDOC");
                return false;
            }
        }

        /// <summary>
        /// Cargamos todos los documentos de los clientes
        /// </summary>
        /// <returns></returns>
        internal bool CargarDocumentos()
        {
            int nRet;
            string sSql = $@"INSERT INTO rclidoc (CDTIPO, CDCLIE, CDNUMERO, DCOBS, CDDEFAULT)
SELECT CDTIPO, CDCLIE, CDNUMERO, CDOBSERV, CDDEFAULT
FROM in_clidoc
WHERE CDFECPROC = {cFormat.StToBD(cGlobales.Hoy)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Documentos Cliente tratados: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarDocumentos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSCliDoc", "Error al insertar RCLIDOC");
                return false;
            }
        }

        #endregion

        #region Atributos del Cliente

        internal bool VaciarAtributos()
        {
            int nRet;
            string sSql = $"DELETE FROM RATRCLIE P WHERE EXISTS (SELECT 1 FROM IN_ATR_CLIE TIN  WHERE TIN.CLFECPROC2 = {cFormat.StToBD(cGlobales.Hoy)} AND P.CLACOD = TIN.CLACOD )";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "VaciarAtributos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "DELAtrClie", "Error al vaciar ATRCLIE");
                return false;
            }
        }

        internal bool CargarAtributos()
        {
            int nRet;
            string sSql = $@"INSERT INTO RATRCLIE (CLACOD, CLBANCA2, CLAFECHA1, CLSUBBANCA2, CLAFECHA2, CLAFECHA3, CLAFECHA4, CLAFECHA5, CLAFECHA6, 
CLAFECHA7, CLAFECHA8, CLAFECHA9, CLAFECHA10, CLSUCURSAL1, CLNUMCUENTA1, CLMONEDA1, CLSALDO1, CLSIGNO1, CLSUCURSAL2, CLNUMCUENTA2, CLMONEDA2, CLSALDO2, 
CLSIGNO2, CLSUCURSAL3, CLNUMCUENTA3, CLMONEDA3, CLSALDO3, CLSIGNO3, CLSUCURSAL4, CLNUMCUENTA4, CLMONEDA4, CLSALDO4, CLSIGNO4, CLSUCURSAL5, 
CLNUMCUENTA5, CLMONEDA5, CLSALDO5, CLSIGNO5, CLSUCURSAL6, CLNUMCUENTA6, CLMONEDA6, CLSALDO6, CLSIGNO6, CLSUCURSAL7, CLNUMCUENTA7, CLMONEDA7, 
CLSALDO7, CLSIGNO7, CLSUCURSAL8, CLNUMCUENTA8, CLMONEDA8, CLSALDO8, CLSIGNO8, CLSUCURSAL9, CLNUMCUENTA9, CLMONEDA9, CLSALDO9, CLSIGNO9, CLSUCURSAL10,
CLNUMCUENTA10, CLMONEDA10, CLSALDO10, CLSIGNO10, CLACOEF1, CLACOEF2, CLACOEF3, CLACOEF4, CLACOEF5, CLACOEF6, CLACOEF7, CLACOEF8, CLACOEF9, CLACOEF10)
SELECT CLACOD, CLBANCA2, CLAFECHA1, CLSUBBANCA2, CLAFECHA2, CLAFECHA3, CLAFECHA4, CLAFECHA5, CLAFECHA6, 
CLAFECHA7, CLAFECHA8, CLAFECHA9, CLAFECHA10, CLSUCURSAL1, CLNUMCUENTA1, CLMONEDA1, CLSALDO1, CLSIGNO1, CLSUCURSAL2, CLNUMCUENTA2, CLMONEDA2, CLSALDO2, 
CLSIGNO2, CLSUCURSAL3, CLNUMCUENTA3, CLMONEDA3, CLSALDO3, CLSIGNO3, CLSUCURSAL4, CLNUMCUENTA4, CLMONEDA4, CLSALDO4, CLSIGNO4, CLSUCURSAL5, 
CLNUMCUENTA5, CLMONEDA5, CLSALDO5, CLSIGNO5, CLSUCURSAL6, CLNUMCUENTA6, CLMONEDA6, CLSALDO6, CLSIGNO6, CLSUCURSAL7, CLNUMCUENTA7, CLMONEDA7, 
CLSALDO7, CLSIGNO7, CLSUCURSAL8, CLNUMCUENTA8, CLMONEDA8, CLSALDO8, CLSIGNO8, CLSUCURSAL9, CLNUMCUENTA9, CLMONEDA9, CLSALDO9, CLSIGNO9, CLSUCURSAL10,
CLNUMCUENTA10, CLMONEDA10, CLSALDO10, CLSIGNO10, CLACOEF1, CLACOEF2, CLACOEF3, CLACOEF4, CLACOEF5, CLACOEF6, CLACOEF7, CLACOEF8, CLACOEF9, CLACOEF10
FROM IN_ATR_CLIE
JOIN RCLIE ON CLACOD = CLCOD
WHERE CLFECPROC2 = {cFormat.StToBD(cGlobales.Hoy)} ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
            {
                cIncidencia.Aviso($"Atributos de Cliente tratados: {nRet}");
                return true;
            }
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "CargarDocumentos", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSAtrClie", "Error al insertar RATRCLIE");
                return false;
            }
        }

        #endregion
    }
}
