using BusComun;
using Comun;
using System;
using System.Data;

namespace BusInchost
{

    public class TyMail
    {
        public string EMCOD;
        public string EMCODMAIL;
        public string EMMAIL;
        public string EMVALIDO;
        public string EMUSRMODIF;
        public string EMORIGEN;
    }

    class MailsDalc : cBase
    {
        public MailsDalc(cConexion pconn)
        {
            conn = pconn;
        }

        #region Tratamiento Unitario
        internal AISDataReader AbrirCursorNuevosMails()
        {
            AISDataReader Dr = new AISDataReader();

            string sSql = $@" select EMCOD, substr(EMCOD, 4, length(EMCOD)-1) as EMCODMAIL, EMMAIL, substr(EMCOD, 1,3) as EMORIGEN
    FROM in_mail
    WHERE EMFECPROC = {cFormat.StToBD(cGlobales.Hoy)}
             ORDER BY EMCOD  ";

            Dr = conn.EjecutarDataReader(sSql);

            Errores = conn.Errores;
            return Dr;
        }

        internal bool FechMail(AISDataReader Dr, ref TyMail Mail)
        {
            try
            {
                Mail = new TyMail();

                if (Dr.Read())
                {
                    Mail.EMCOD = Dr["EMCOD"].ToString();
                    Mail.EMCODMAIL = Dr["EMCODMAIL"].ToString();
                    Mail.EMMAIL = Dr["EMMAIL"].ToString();
                    Mail.EMVALIDO = "1";
                    Mail.EMORIGEN = Dr["EMORIGEN"].ToString();

                    return true;
                }
                else
                    return false;

            }
            catch (Exception e)
            {
                Errores.Agregar(Const.ERROR_BASE_DATOS, e.Message, "FechMail", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "FETCHMAIL", "Error al recuperar Mail");
                return false;
            }
        }

        internal TipoAccion ObtengoMailBD(TyMail BDMail)
        {
            DataSet Ds = new DataSet();

            string sSql = $@" SELECT EMCOD, EMCODMAIL, EMMAIL, EMVALIDO, EMORIGEN, EMUSRMODIF
            FROM RMAILS  
            WHERE EMCOD = {cFormat.StToBD(BDMail.EMCOD)} 
              AND EMCODMAIL = {cFormat.StToBD(BDMail.EMCODMAIL)}
              AND EMORIGEN = {cFormat.StToBD(BDMail.EMORIGEN)}";

            Ds = conn.EjecutarQuery(sSql, "RMAIL");

            if (conn.Errores.Cantidad() == 0)
            {
                if (Ds.Tables["RMAIL"].Rows.Count > 0)
                {
                    DataRow Dr = Ds.Tables["RMAIL"].Rows[0];

                    BDMail.EMCOD = Dr["EMCOD"].ToString();
                    BDMail.EMCODMAIL = Dr["EMCODMAIL"].ToString();
                    BDMail.EMMAIL = Dr["EMMAIL"].ToString();
                    BDMail.EMVALIDO = Dr["EMVALIDO"].ToString();
                    BDMail.EMORIGEN = Dr["EMORIGEN"].ToString();
                    BDMail.EMUSRMODIF = Dr["EMUSRMODIF"].ToString().Trim();

                    return TipoAccion.Modificacion;
                }
                else
                    return TipoAccion.Alta;
            }
            else
            {
                cIncidencia.Generar(conn.Errores, "SELMAIL", "Error en Select de Mails. Cliente: " + BDMail.EMCOD + " - Codigo Mail: " + BDMail.EMCODMAIL);
                return TipoAccion.Error;
            }
        }

        internal bool InsertaMailBD(TyMail Mail)
        {
            int nRet;

            string sSql = $@" INSERT INTO RMAILS (EMCOD, EMCODMAIL, EMMAIL, EMVALIDO, EMORIGEN) 
VALUES ( {cFormat.StToBD(Mail.EMCOD)}, {cFormat.StToBD(Mail.EMCODMAIL)}, {cFormat.StToBD(Mail.EMMAIL)}, {cFormat.StToBD(Mail.EMVALIDO)}, 
{cFormat.StToBD(Mail.EMORIGEN)} )";

            nRet = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "InsertaMailBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "INSMAIL", "Error al insertar Mail. Cliente: " + Mail.EMCOD + " - Codigo Mail: " + Mail.EMCODMAIL);
                return false;
            }
        }

        internal bool ModificaMailBD(TyMail Mail)
        {
            int nRet;

            string sSql = $@" UPDATE RMails SET  
             EMMAIL = {cFormat.StToBD(Mail.EMMAIL)}, 
             EMVALIDO = {cFormat.StToBD(Mail.EMVALIDO)},
             EMUSRMODIF = NULL, 
             EMFECMODIF = NULL, 
             EMHORAMODIF = NULL 
             WHERE EMCOD = { cFormat.StToBD(Mail.EMCOD) } 
               AND EMCODMAIL = { cFormat.StToBD(Mail.EMCODMAIL) } 
               AND EMORIGEN = { cFormat.StToBD(Mail.EMORIGEN) } ";

            nRet = conn.EjecutarQuery(sSql);
            if (conn.Errores.Cantidad() == 0)
                return true;
            else
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaMailBD", Const.SEVERIDAD_Alta);
                cIncidencia.Generar(Errores, "UPDMail", "Error al modificar Mail. Cliente: " + Mail.EMCOD + " - Codigo Mail: " + Mail.EMCODMAIL);
                return false;
            }
        }

        #endregion

        #region Tratamiento Masivo

        /// <summary>
        /// Borra los eMails de la tabla productiva que se van a cargar en el día de hoy
        /// </summary>
        internal void BorrareMails()
        {
            string sSql = $@"DELETE FROM RMAILS P 
WHERE p.EMORIGEN in ('COM', 'LSG') 
    AND EXISTS (SELECT 1 FROM IN_MAIL I WHERE I.EMFECPROC = {cFormat.StToBD(cGlobales.Hoy)} AND i.EMCOD = p.EMCOD)";

            conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaGarantiaBD", Const.SEVERIDAD_Alta);
            }
        }

        /// <summary>
        /// Inserta masivamente los eMails que han llegado en el día de Hoy
        /// </summary>
        internal void InsertareMails()
        {
            string sSql = $@"insert into RMAILS (EMCOD, EMCODMAIL, EMMAIL, EMORIGEN)
SELECT EMCOD, SUBSTR(EMCOD, 4) as EMCODMAIL, EMMAIL, SUBSTR(EMCOD, 1, 3) EMORIGEN
FROM IN_MAIL 
WHERE EMFECPROC = {cFormat.StToBD(cGlobales.Hoy)} AND EXISTS (SELECT 1 FROM RCLIE WHERE CLCOD = EMCOD) ";

            int res = conn.EjecutarQuery(sSql);

            if (conn.Errores.Cantidad() > 0)
            {
                Errores.Agregar(conn.Errores.Item(0).nCodigo, conn.Errores.Item(0).sDescripcion, "ModificaGarantiaBD", Const.SEVERIDAD_Alta);
            }
            else
            {
                cIncidencia.Aviso($"eMails tratados: {res}");
            }

        }

        #endregion

    }
}
